package canal

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/samuel/go-zookeeper/zk"
)

type ZkConn struct {
	Conn     *zk.Conn
	initPath string
}

type DumpSyncMetaData struct {
	Dumpfile string
	Name     string
	Pos      uint32
	Offset   int64
}

func ZkConnection(zkPath []string, initPath string) (*ZkConn, error) {

	if initPath[0] != '/' || len(initPath) == 0 {
		return &ZkConn{}, errors.New("initPath must len > 0 or begin with / ")
	}

	if initPath[len(initPath)-1] == '/' {
		initPath = initPath[0 : len(initPath)-1]
	}

	conn, _, err := zk.Connect(zkPath, time.Second*5)
	if err != nil {
		return &ZkConn{}, err
	}

	exists, _, err := conn.Exists(initPath)
	if err != nil {
		return &ZkConn{}, err
	}

	if !exists {
		log.Warningf("initPath %s was not exist ,Now create it \n", initPath)
		_, err = conn.Create(initPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return &ZkConn{}, err
		}
		log.Warningf("Zookeeper Connect: %s, initPath: %s \n", zkPath, initPath)
	}

	return &ZkConn{Conn: conn, initPath: initPath}, nil
}

// flag: zk.FlagEphemeral
func (z *ZkConn) Create(path string, data string, flag int32) error {
	var withData []byte = nil
	if data != "" {
		withData = []byte(data)
	}

	exists, _, err := z.Conn.Exists(z.fullPath(path))
	if err != nil {
		return err
	}

	if !exists {
		_, err := z.Conn.Create(z.fullPath(path), withData, flag, zk.WorldACL(zk.PermAll))
		log.Warningf("Zookeeper Create: %s %s, %v\n", z.fullPath(path), data, err)

		return err
	}

	log.Infof("zk Node %s have exists \n", z.fullPath(path))
	return nil
}

func (z *ZkConn) Get(path string) (string, error) {
	data, _, err := z.Conn.Get(z.fullPath(path))
	log.Infof("Zookeeper Get: %s %s, %v \n", z.fullPath(path), data, err)

	return string(data), err
}

func (z *ZkConn) Set(path string, data string) error {

	_, err := z.Conn.Set(z.fullPath(path), []byte(data), -1)
	if err != nil {
		log.Error("SET TO ZK WAS WRONG")
		return err
	}
	log.Infof("Zookeeper Set: %s %s, %v \n", z.fullPath(path), data, err)

	return err
}

func (z *ZkConn) Del(path string) error {
	err := z.Conn.Delete(z.fullPath(path), -1)
	log.Infof("zookeeper Del: %s, %v \n", z.fullPath(path), err)

	return err
}

func (z *ZkConn) Exist(path string) (bool, error) {
	exists, _, err := z.Conn.Exists(z.fullPath(path))
	log.Infof("zookeeper Exist: %s %t, %v \n", z.fullPath(path), exists, err)

	return exists, err
}

func (z *ZkConn) Children(path string) ([]string, error) {
	list, _, err := z.Conn.Children(z.fullPath(path))
	log.Infof("zookeeper Children: %s %v, %v \n", z.fullPath(path), list, err)

	return list, err
}

func (z *ZkConn) Replace(path string, data string, flag int32) error {
	exist, existErr := z.Exist(path)
	if existErr != nil {
		return existErr
	}

	if !exist {
		return z.Create(path, data, flag)
	}

	return z.Set(path, data)
}

func (z *ZkConn) CreateNX(path string, data string, flag int32) error {
	exist, existErr := z.Exist(path)
	if existErr != nil {
		return existErr
	}

	if !exist {
		return z.Create(path, data, flag)
	}

	return fmt.Errorf("node already exist %s:%s", path, data)
}

func (z *ZkConn) Close() {
	z.Conn.Close()
}

func (z *ZkConn) fullPath(path string) string {
	if path == "" {
		return z.initPath
	}

	if path[0] == '/' {
		return z.initPath + path
	}
	return z.initPath + "/" + path
}

func DumpsyncMetaDataEncode(info DumpSyncMetaData) string {
	return fmt.Sprintf("%s:%s:%d:%d", info.Dumpfile, info.Name, info.Pos, info.Offset)
}

func DumpsyncMetaDataDecode(str string) (DumpSyncMetaData, error) {
	info := DumpSyncMetaData{}
	strs := strings.Split(str, ":")
	if len(strs) != 4 {
		return info, errors.New("Decode Data From zk Wrong")
	}
	log.Debug("DumpsyncMetaDataDecode:", strs, "\n")
	log.Debug("DumpsyncMetaDataDecode: strs[0] is %s,strs[1] is %s,strs[2] is %s,strs[3] is %s\n", strs[0], strs[1], strs[2], strs[3])
	info.Dumpfile = strs[0]
	info.Name = strs[1]
	pos, _ := strconv.ParseUint(strs[2], 10, 32)
	info.Pos = uint32(pos)
	info.Offset, _ = strconv.ParseInt(strs[3], 10, 64)
	log.Debug("DumpsyncMetaDataDecode: ", info, "\n")
	return info, nil
}

type KeepAliveNode struct {
	IP         string
	Port       string
	Topic      string
	TimeSecond int64
}

func KeepAliveNodeEncode(node KeepAliveNode) string {
	return fmt.Sprintf("%s:%s:%s:%d", node.IP, node.Port, node.Topic, node.TimeSecond)
}

func KeepAliveNodeDecode(str string) (KeepAliveNode, error) {
	node := KeepAliveNode{}
	strs := strings.Split(str, ":")
	if len(strs) != 4 {
		return node, errors.New("Decode Data From zk Wrong")
	}
	node.IP = strs[0]
	node.Port = strs[1]
	node.Topic = strs[2]
	timesecond, _ := strconv.ParseInt(strs[3], 10, 64)
	node.TimeSecond = timesecond

	fmt.Printf("KeepAliveNode: IP: %s,Port %s,Topic %s,TimeSecond %d \n", node.IP, node.Port, node.Topic, node.TimeSecond)

	return node, nil
}
