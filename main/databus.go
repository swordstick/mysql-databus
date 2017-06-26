package main

import (
	"canal"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"plugins"
	"runtime"
	"syscall"
	"time"
)

var (
	configFile = flag.String("c", "./databus.toml", "config file, Usage<-c file>")
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	flag.Parse()
	var cfg *canal.Config
	var err error

	if cfg, err = canal.NewConfigWithFile(*configFile); err != nil {
		log.Panicf("parse config file failed(%s): %s", *configFile, err)
	}

	log.Printf("DumpClock is %d", cfg.DumpClock)
	if cfg.DumpClock == 0 {
		cfg.DumpClock = 60
	}

	log.Printf("DumpClock Change to %d", cfg.DumpClock)

	fmt.Printf("%s is Topic", cfg.Topic)
	// canal.NewSyncProducerToKafka(cfg.Topic)

	/* 暂时屏蔽字段过滤，和表名转换功能
	if err := canal.ParseFilter(*configFile); err != nil {
		log.Panicf("parse filterclos file failed(%s): %s", *configFile, err)
	}

	if err := canal.ParseOptimus(*configFile); err != nil {
		log.Panicf("parse optimus file failed(%s): %s", *configFile, err)
	}

	*/

	go tranferData(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc
	canal.MasterSave.Save(true)
	fmt.Println("DTASHIP: Bye Bye ..!!")
}

func tranferData(cfg *canal.Config) {

	if !cfg.Mirrorenable {
		for i := 0; ; i++ {
			startCanal(cfg)
		}
	}

	if cfg.Mirrorenable {
		for i := 0; ; i++ {
			// 检查自身是否运行节点,若是则返回true，下面的startCanal正式启动,否则进入死循环
			startOrnot, err := checkMasterAlive(cfg)
			if err != nil {
				log.Println("checkMaster: I'm Fail!!!", err)
			}
			if startOrnot {
				SaveKeepAlive(cfg)
				startCanal(cfg)
				// 为防止脑裂，一旦某节点从运行状态退出，10分钟以内不会再尝试探测及修改ZK的KEEPALIVE NODE信息
				// 从运行状态退出后，过10分钟，将再次进入从节点角色
				time.Sleep(time.Second * 600)
			}
			// 未进入之前，每次探测时间20S，注意和前面的对比10分钟对比，必须远小于前者
			time.Sleep(time.Second * 20)
		}
	}
}

func startCanal(cfg *canal.Config) bool {

	var c *canal.Canal
	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("Create New Canal err %v", err)
		os.Exit(1)
	}

	// fmt.Printf("after NewCanal create") //debug

	handler := getRowsEventHandler(cfg)
	ddlhandler := getQueryEventHandler(cfg)

	if handler == nil || ddlhandler == nil || c == nil {
		if handler != nil {
			handler.Close()
		}
		if ddlhandler != nil {
			ddlhandler.Close()
		}
		if c != nil {
			c.Close()
		}
		return false
	}

	c.RegRowsEventHandler(handler)
	c.RegQueryEventHandler(ddlhandler)

	// 若并非初始化Dump，说明之前已经生产，必须读取上次保存的表结构
	if !c.TryDumpOrNot() {
		c.ParseTableSchema()
	}

	if c.TryDumpOrNot() {
		canal.Readydumpfile = "DB.Data" + time.Now().Format(canal.Layout) + ".Json"
		dphandler := getRowsEventDumpHandler(cfg, canal.Readydumpfile)
		if dphandler == nil {
			return false
		}
		c.RegRowsEventDumpHandler(dphandler)
	}

	canalquit := c.Start()
	defer ddlhandler.Close()
	defer handler.Close()
	defer c.Close()

	alivequit := make(chan error)
	// 启动节点注册GOROUTINE，为防止被其他操作阻塞主线程，所以注册需要独立线程完成
	go SaveKeepAliveGo(cfg, alivequit)

	SaveTimer := time.NewTicker(2 * time.Second)
	DumpTimer := time.NewTicker(time.Duration(cfg.DumpClock) * time.Second)
	DelDumpFileTimer := time.NewTicker(time.Duration(cfg.DumpClock) * time.Second)
	defer SaveTimer.Stop()
	defer DumpTimer.Stop()
	defer DelDumpFileTimer.Stop()

	for {
		select {
		case <-DelDumpFileTimer.C:
			c.DelDumpFile()
		case <-SaveTimer.C:
			c.SaveConfig()
		case <-DumpTimer.C:
			if !c.TryDumpClockOrNot() {
				continue
			}
			{
				canal.Readydumpfile = "DB.Data" + time.Now().Format(canal.Layout) + ".Json"
				dphandler := getRowsEventDumpHandler(cfg, canal.Readydumpfile)
				c.RegRowsEventDumpHandler(dphandler)
				fmt.Printf("\nInto TryDumpForClock!!\n")
				go c.TryDumpForClock()
			}
		case syncErr := <-canalquit:
			log.Printf("canal quit: %v", syncErr)
			return false
		case <-alivequit:
			log.Print("Keep Alive: I'm Not Master again,Clear everything and Quit ! \n")
			return false
		}
	}
	return true
}

func getRowsEventHandler(cfg *canal.Config) canal.RowsEventHandler {
	//var handler canal.RowsEventHandler
	handler := plugins.NewKafkaSyncHandler(cfg)
	if handler != nil {
		return handler
	} else {
		return nil
	}
}

func getQueryEventHandler(cfg *canal.Config) canal.QueryEventHandler {
	var handler canal.QueryEventHandler
	handler = plugins.NewKafkaSyncHandler(cfg)
	if handler != nil {
		return handler
	} else {
		return nil
	}
}

func getRowsEventDumpHandler(cfg *canal.Config, Dumpfile string) canal.RowsEventDumpHandler {
	//var handler canal.RowsEventHandler for dump parse
	handler := plugins.NewDataFileHandler(cfg, Dumpfile)
	if handler != nil {
		return handler
	} else {
		return nil
	}
}

func SaveKeepAlive(cfg *canal.Config) (bool, error) {

	initPath := "/Databus"
	zkConn, connErr := canal.ZkConnection(cfg.ZkPath, initPath)
	if connErr != nil {
		return false, connErr
	}
	defer zkConn.Close()

	zkConn.Create(cfg.KeepAlivepath, "", 0)
	zkConn.Create(cfg.KeepAlivepath+"/"+cfg.Topic, "", 0)

	Time := time.Now()

	keepnode := canal.KeepAliveNode{IP: cfg.MonitorIP, Port: cfg.MonitorPort, Topic: cfg.Topic, TimeSecond: Time.Unix()}
	zkConn.Set(cfg.KeepAlivepath+"/"+cfg.Topic, canal.KeepAliveNodeEncode(keepnode))

	return true, nil
}

// 首先分析，假如发现最新注册不是自己，这说明，在本server运行过程中，和ZK有通信失败情况，已经被人抢占了主节点属性，此时应该写日志报错并退出回到从节点状态
func SaveKeepAliveGo(cfg *canal.Config, quit chan error) error {
	CheckTimer := time.NewTicker(30 * time.Second)
	defer CheckTimer.Stop()
	for {
		<-CheckTimer.C

		// 定时向ZK登记注册相关自身节点
		if cfg.Mirrorenable {
			// 检查自己是否主节点
			startOrnot, _ := checkMasterAlive(cfg)
			if !startOrnot && canal.KeepAliveErrNum == 3 {
				canal.KeepAliveErrNum = 0
				quit <- fmt.Errorf("SaveKeepAliveGo of %s: When SaveKeepAlive, find myself is not KeepAlive node again,so quit to follow node", cfg.MonitorIP)
				break
			}
			if !startOrnot {
				break
			}
			// 注册
			_, err := SaveKeepAlive(cfg)
			if err != nil {
				log.Println("SaveKeepAlive: I'm Fail!!", err)
			}
		}
	}

	return nil
}

func checkMasterAlive(cfg *canal.Config) (bool, error) {

	if canal.KeepAliveErrNum > 2 {
		canal.KeepAliveErrNum = 0
	}

	initPath := "/Databus"
	zkConn, connErr := canal.ZkConnection(cfg.ZkPath, initPath)
	if connErr != nil {
		canal.KeepAliveErrNum = canal.KeepAliveErrNum + 1
		return false, connErr
	}

	defer zkConn.Close()

	exist, err := zkConn.Exist(cfg.KeepAlivepath + "/" + cfg.Topic)
	if err != nil {
		canal.KeepAliveErrNum = canal.KeepAliveErrNum + 1
		return false, err
	}
	if !exist {
		log.Printf("MasterAlive message First Auth to ZK !! \n")
		ok, _ := SaveKeepAlive(cfg)
		if ok {
			canal.KeepAliveErrNum = 0
			return true, nil
		}
	}

	value, _ := zkConn.Get(cfg.KeepAlivepath + "/" + cfg.Topic)
	keepnode, err := canal.KeepAliveNodeDecode(value)
	if err != nil {
		canal.KeepAliveErrNum = canal.KeepAliveErrNum + 1
		return false, err
	}

	fmt.Printf("keepnode is %v", keepnode)
	fmt.Printf("\n Last Node time is %d \n", keepnode.TimeSecond)
	fmt.Printf("Last Node Out time is %d \n", keepnode.TimeSecond+cfg.Alivetimeout)

	Time := time.Now()
	if keepnode.IP == cfg.MonitorIP && keepnode.Topic == cfg.Topic {
		canal.KeepAliveErrNum = 0
		return true, nil
	}

	fmt.Printf("Noew Node time is %d \n", Time.Unix())

	if (keepnode.TimeSecond + cfg.Alivetimeout) < Time.Unix() {
		_, err := SaveKeepAlive(cfg)
		if err != nil {
			log.Println("SaveKeepAlive: I'm Fail!!", err)
			canal.KeepAliveErrNum = canal.KeepAliveErrNum + 1
			return false, err
		}
		canal.KeepAliveErrNum = 0
		return true, nil
	}

	canal.KeepAliveErrNum = canal.KeepAliveErrNum + 1
	return false, nil
}
