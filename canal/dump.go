package canal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
	"transfer"

	"dump"

	"path"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/schema"
)

//"errors"

var Roll chan int

type dumpParseHandler struct {
	c    *Canal
	name string
	pos  uint64
}

type dumpParseHandlerGo struct {
	c      *Canal
	db     string
	table  string
	values []string
}

func NewdumpParseHandlerGo(h *dumpParseHandler, db string, table string, values []string) dumpParseHandlerGo {
	t := new(dumpParseHandlerGo)
	t.c = h.c
	t.db = db
	t.table = table
	t.values = values
	return *t
}

func (h *dumpParseHandlerGo) DataGo() error {

	if h.c.isClosed() {
		return errCanalClosed
	}

	db := h.db
	table := h.table
	values := h.values

	tableInfo, err := h.c.GetTable(db, table)
	if err != nil {
		log.Errorf("Get %s.%s information err: %v", db, table, err)
		Roll <- 110
		return errors.Trace(err)
	}

	vs := make([]interface{}, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					log.Errorf("Parse row %v at %d error %v, skip", values, i, err)
					Roll <- 110
					return dump.ErrSkip
				}
				vs[i] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					log.Errorf("Parse row %v at %d error %v, skip", values, i, err)
					Roll <- 110
					return dump.ErrSkip
				}
				vs[i] = f
			} else {
				log.Errorf("Parse row %v error, invalid type at %d, skip", values, i)
				Roll <- 110
				return dump.ErrSkip
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	events := newRowsEvent(tableInfo, InsertAction, [][]interface{}{vs})
	if err := h.c.travelRowsEventDumpHandler(events); err == nil {
		return nil
	} else {
		Roll <- 110
		return err
	}
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

// update
func (h *dumpParseHandler) Data(db string, table string, values []string) error {
	// log.Debug(" into dumpParseHandler.Data  \n") //debug

	hGo := NewdumpParseHandlerGo(h, db, table, values)
	h.c.dumppool.Run(hGo)
	// log.Debug(" pushed to Pool  \n") //debug

	select {
	case <-Roll:
		return errors.Trace(fmt.Errorf("Dumper Go routine failed"))
	default:
		return nil
	}
}

func (c *Canal) AddDumpDatabases(dbs ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddDatabases(dbs...)
}

func (c *Canal) AddDumpTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddTables(db, tables...)
}

func (c *Canal) AddDumpIgnoreTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddIgnoreTables(db, tables...)
}

func (c *Canal) tryDump() error {

	c.Firstdumping = true
	defer func() { c.Firstdumping = false }()

	// 调整len(c.master.Name) > 0 && c.master.Position > 0 为下面
	/*
		if len(c.master.DumpPosName) > 0 && c.master.DumpPos > 0 {
			// we will sync with binlog name and position
			log.Warningf("Skip dump, use last binlog replication pos (%s, %d)", c.master.Name, c.master.Position)
			return nil
		}

		if c.dumper == nil {
			log.Warningf("Skip dump, no mysqldump")
			return nil
		}
	*/

	h := &dumpParseHandler{
		c: c}

	start := time.Now()
	log.Warningf("Try dump MySQL and parse")
	if err := c.dumper.DumpAndParse(h); err != nil {
		return errors.Trace(err)
	}

	c.dumppool.Shutdown()

	log.Warningf("Dump MySQL and parse OK, use %0.2f seconds, start binlog replication at (%s, %d)",
		time.Now().Sub(start).Seconds(), h.name, h.pos)

	// 更新offset
	p, err := NewPartitionConsumer(c.cfg.Topic, c.cfg.Brokers, true)
	if err != nil {
		return errors.Trace(err)
	}

	offset, err := p.GetNewOffset()
	if err != nil {
		return err
	}

	defer p.master.Close()
	defer p.client.Close()

	// 文件传输到其他节点，如果传输失败，则整个DUMP失效，程序退出
	// 启动第一次，保证严格的执行

	if c.cfg.Mirrorenable {
		copy := make(chan bool)
		log.Info("Start TransFileBySsh :")
		go c.TransFileBySsh(Readydumpfile, copy)
		if !(<-copy) {
			log.Fatalf("Trans File By SSH to remote %s Fail, This is the first Dump, So Bye.. Bye.. ,Please check network or filedir are right or not!!!", c.cfg.Sshhostname)
		}
	}

	// 变更对外pos信息

	time.Sleep(1 * time.Second)

	c.master.Update(h.name, uint32(h.pos), h.name, uint32(h.pos), offset, Readydumpfile)
	c.master.Save(true)

	log.Infof("Readydumpfile : %s", Readydumpfile)

	// 更新ZK的DUMP发布信息
	err = AuthZkDumpInfo(c)
	if err != nil {
		log.Info("FAIL AUTH DUMP OFFSET to ZK")
	} else {
		log.Info("SUCCESS AUTH DUMP OFFSET to ZK")
	}

	log.Info("Have AUTH OFFSET ... to ZK")

	fmt.Println("Offset is", c.master.Dumpoffset, "bonlog pos is", c.master.DumpPos, c.master.Pos)

	c.AddDumpIndex(Readydumpfile)
	log.Infof("Add DumpFile %s to dump.Index", Readydumpfile)

	return nil
}

func (c *Canal) TryDumpForClock() error {

	c.dumppool = NewPool(ThreadforDump)
	defer c.dumppool.Shutdown()

	c.dumping = true
	defer func() { c.dumping = false }()

	h := &dumpParseHandler{
		c: c}

	p, err := NewPartitionConsumer(c.cfg.Topic, c.cfg.Brokers, true)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	defer p.master.Close()
	defer p.client.Close()

	start := time.Now()
	log.Info("CLOCK: try dump MySQL and parse to File")
	if err := c.dumper.DumpAndParse(h); err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	log.Infof("CLOCK: dump MySQL and parse OK, use %0.2f seconds, start binlog replication at (%s, %d)",
		time.Now().Sub(start).Seconds(), h.name, h.pos)

	// 计算出pos对应在Kafka中的offset

	offset, err := p.Parse(h.name, h.pos, c)
	if err != nil {
		log.Errorf("Have not get Clock dump offset!!!")
		log.Error(err)
		return errors.Trace(err)
	}

	// 文件传输到其他节点，如果传输失败，则此次Dump失效，并发出告警

	if c.cfg.Mirrorenable {
		copy := make(chan bool)
		log.Info("Start TransFileBySsh :")
		go c.TransFileBySsh(Readydumpfile, copy)
		if !(<-copy) {
			log.Errorf("Trans File By SSH to remote %s Fail, This is the first Dump, So ignore this dump ,Please check network or filedir are right or not!!!", c.cfg.Sshhostname)
		}
		log.Info("End TransFileBySsh :")
	}

	// 变更对外pos信息

	c.master.Update("", 0, h.name, uint32(h.pos), offset, Readydumpfile)
	c.master.Save(true)

	log.Infof("Readydumpfile : %s", Readydumpfile)

	// 更新ZK的DUMP发布信息
	err = AuthZkDumpInfo(c)
	if err != nil {
		log.Info("FAIL AUTH DUMP OFFSET to ZK")
	} else {
		log.Info("SUCCESS AUTH DUMP OFFSET to ZK")
	}

	log.Infof("Offset is %d, bonlog pos is %d,%d", c.master.Dumpoffset, c.master.Dumpfile, c.master.DumpPos)

	c.AddDumpIndex(Readydumpfile)
	log.Infof("Add DumpFile %s to dump.Index", Readydumpfile)

	return nil
}

func (c *Canal) TryDumpOrNot() bool {

	var dumpfileOk bool

	if transfer.IsFileExists(path.Join(c.cfg.DataDir, c.master.Dumpfile)) {
		fin, err := os.OpenFile(path.Join(c.cfg.DataDir, c.master.Dumpfile), os.O_RDWR, 0666)
		// fin, err := os.Open(path.Join(dir, filename))
		if err != nil {
			log.Error("TryDump: DumpFile Exists But Can not Open !! \n")
			panic(err)
		}
		fininfo, _ := fin.Stat()
		if fininfo.Size() != 0 {
			dumpfileOk = true
		}
	}

	// 调整len(c.master.Name) > 0 && c.master.Position > 0 为下面
	// 加入zk说描述的DUMP文件不存在，则必须进行一次Dump初始化，不排除文件可能被删除
	if len(c.master.DumpPosName) > 0 && c.master.DumpPos > 0 && dumpfileOk {
		// we will sync with binlog name and position
		log.Warningf("skip dump, use last binlog replication pos (%s, %d)", c.master.Name, c.master.Position)
		c.Firstdumping = false
		return false
	}

	if c.dumper == nil {
		log.Info("skip dump, no mysqldump \n Bye .. Bye .. !!\n")
		os.Exit(-1)
	}

	return true
}

func (c *Canal) TryDumpClockOrNot() bool {
	if c.Firstdumping {
		return false
	}
	if c.dumping {
		log.Error("Clock Dump is running OR Have been shutdown ,skip start again!!!")
		return false
	}
	if c.dumper == nil {
		log.Error("skip dump, no mysqldump")
		return false
	}

	return true
}

func (c *Canal) TransFileBySsh(filename string, copy chan bool) {

	paths := path.Join(c.cfg.DataDir, filename)
	mirror, err := transfer.NewSftp(c.cfg.Sshhostname, c.cfg.Sshport, c.cfg.Sshuser, c.cfg.Sshpasswd)
	if err != nil {
		errors.Trace(err)
		copy <- false
	}

	defer mirror.CloseSftp()

	log.Infof("Create TransFilebySSH sftp for remote host %s", c.cfg.Sshhostname)

	transok, err := mirror.TransOrNot(paths, c.cfg.RemoteDir)
	if err != nil {
		errors.Trace(err)
		copy <- false
	}

	var transfile bool

	if transok {
		for i := 0; i < 3; i++ {
			time.Sleep(30 * time.Second)
			transfile, err = mirror.TransFile(paths, c.cfg.RemoteDir)
			if err != nil && !transfile {
				errors.Trace(err)
				continue
			} else {
				break
			}
		}

		if transfile {
			fmt.Println("TRANSFILE OVER ,AND SUCCESS!!!")
			copy <- true
		}
	}

	copy <- false
}

func AuthZkDumpInfo(c *Canal) error {

	initPath := "/Databus"
	zkConn, connErr := ZkConnection(c.cfg.ZkPath, initPath)
	if connErr != nil {
		return connErr
	}
	defer zkConn.Close()

	zkConn.Create(c.cfg.NodeName, "", 0)
	zkConn.Create(c.cfg.NodeName+"/"+c.cfg.Topic, "", 0)

	Dumpmetadata := DumpSyncMetaData{Dumpfile: c.master.Dumpfile, Pos: c.master.DumpPos, Name: c.master.DumpPosName, Offset: c.master.Dumpoffset}
	zkConn.Set(c.cfg.NodeName+"/"+c.cfg.Topic, DumpsyncMetaDataEncode(Dumpmetadata))

	return nil
}

func GetZkDumpInfo(c *Canal) (*DumpSyncMetaData, error) {

	initPath := "/Databus"
	zkConn, connErr := ZkConnection(c.cfg.ZkPath, initPath)
	if connErr != nil {
		return nil, connErr
	}
	defer zkConn.Close()

	value, _ := zkConn.Get(c.cfg.NodeName + "/" + c.cfg.Topic)
	log.Debug("GetZkDumpInfo: ZKDUMPINFO is  \n", value, "\n")
	zkDumpMetadata, err := DumpsyncMetaDataDecode(value)
	if err != nil {
		return &DumpSyncMetaData{}, err
	}

	return &zkDumpMetadata, nil
}

func ComputeMasterInfoWithZK(c *Canal) error {
	// 获取zk和kafak信息，进行分析

	DumpInfo, err := GetZkDumpInfo(c)
	if err != nil {
		log.Info("GetZkDumpInfo exit")
		return err
	}

	// 获取KAFKA数据

	p, err := NewPartitionConsumer(c.cfg.Topic, c.cfg.Brokers, false)
	if err != nil {
		log.Info("NewPartitionConsumer exit")
		return err
	}

	defer p.master.Close()
	defer p.client.Close()

	Newoffset, err := p.GetNewOffset()
	if err != nil {
		log.Info("GetNewOffset exit")
		return err
	}

	log.Debugf("Newoffset is %d\n", Newoffset)

	Oldoffset, err := p.GetOldOffset()
	if err != nil {
		log.Info("GetOldOffset exit")
		return err
	}

	log.Debugf("Oldoffset is %d\n", Oldoffset)

	// 说明kafka的位置都没变过，那就直接采用ZKDumpInfo的信息好了
	// 假如Newoffset 可以小于 Oldoffset ，则这里的算法要调整
	if Newoffset == Oldoffset {

		fmt.Printf("ComputeMasterInfoWithZK: Into Newoffset == Oldoffset\n")
		c.master = &masterInfo{Name: DumpInfo.Name, Position: DumpInfo.Pos, Addr: c.cfg.Addr, DumpPosName: DumpInfo.Name, DumpPos: DumpInfo.Pos, Dumpoffset: DumpInfo.Offset, Dumpfile: DumpInfo.Dumpfile}

		fmt.Println(c.master)
		return nil
	}

	Name, Pos, err := p.GetPosKeyWithKafkaOffset(c.cfg.Topic, Newoffset-1)
	if err != nil {
		log.Warning("GetPosKeyWithKafkaOffset exit")
		log.Warning(err)
		return err
	}

	fmt.Printf("ComputeMasterInfoWithZK:  Into KafkaOffset Name: %s,Pos %d", Name, Pos)

	c.master = &masterInfo{Name: Name, Position: Pos, Addr: c.cfg.Addr, DumpPosName: DumpInfo.Name, DumpPos: DumpInfo.Pos, Dumpoffset: DumpInfo.Offset, Dumpfile: DumpInfo.Dumpfile}

	return nil
}

func ComputeMasterInfoWithFile(c *Canal) {

	//获取本地文件
	var err error
	if c.master, err = loadMasterInfo(c.masterInfoPath()); err != nil {
		log.Errorf("PLEASE CHECK MASTER.INFO STRUCT,MayBe Decoder Fail!!")
		panic(err)
		//return errors.Trace(err)
	} else if len(c.master.Addr) != 0 && c.master.Addr != c.cfg.Addr {
		log.Warningf("MySQL addr %s in old master.info, but new %s, reset,they should be the same,Bye .. Bye ..", c.master.Addr, c.cfg.Addr)
		// may use another MySQL, reset
		// c.master = &masterInfo{}
		panic("DATABUS: Bye .. Bye ..")
	}

}

func (c *Canal) DelDumpFile() error {

	file, err := os.OpenFile(path.Join(c.cfg.DataDir, "dump.index"), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	filerReader := bufio.NewReader(file)

	// 获取剔除了filename的文件内容
	var newfile []string
	for {
		line, err := filerReader.ReadString('\n')
		if err == io.EOF {
			break
		}
		newfile = append(newfile, line[0:len(line)-1])
	}

	var filename string

	if len(newfile) < 3 {
		return nil
	}

	for n := 0; n < len(newfile)-2; n++ {
		filename = newfile[n]
		if transfer.FileTransing[filename] == true {
			continue
		}
		go os.Remove(path.Join(c.cfg.DataDir, filename))

		c.DelDumpIndex(filename)

		log.Infof("Dump File %s was Delete!!!", filename)
	}

	return nil
}

func (c *Canal) AddDumpIndex(filename string) error {
	c.dpfile.Lock()
	defer c.dpfile.Unlock()

	file, err := os.OpenFile(path.Join(c.cfg.DataDir, "dump.index"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString(filename + "\n")
	return nil
}

func (c *Canal) DelDumpIndex(filename string) error {
	c.dpfile.Lock()
	defer c.dpfile.Unlock()

	file, err := os.OpenFile(path.Join(c.cfg.DataDir, "dump.index"), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	filerReader := bufio.NewReader(file)
	var newfile string

	// 获取剔除了filename的文件内容
	for {
		line, err := filerReader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if line[0:len(line)-1] == filename {
			continue
		}
		newfile = newfile + line
	}

	// 清空文件
	err = file.Truncate(0)
	if err != nil {
		return err
	}

	// 写入文件
	_, err = file.WriteAt([]byte(newfile), 0)
	if err != nil {
		return err
	}
	return nil
}
