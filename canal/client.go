package canal

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ngaut/log"
)

const (
	CtlClearScreen     string        = "\033[2J"
	CtlCursorReset     string        = "\033[H"
	CtlNewEnter        string        = "\r\033[K"
	CtlNewline         string        = "\r\n\033[K"
	telnetCmdPrompt    string        = "\r\nCMD> "
	ExitString         string        = "exit"
	telnetCmdFreshTime time.Duration = 1
)

var telnetCmdHelpStr string = `
=======================================================================
auth				--- auth passwd
exit				--- exit telnet main
info				--- view info 
					--- example "info"
loglevel			--- set loglevel 
					--- example 
					--- "loglevel debug"
					--- "loglevel info"
					--- "loglevel warning"
off					--- "off clockdump"	    
-----------------------------------------------------------------------`

type Cmdprocess struct {
	ln   net.Listener
	ip   string
	port string
}

func NewCMDListener(ip string, port string) (*Cmdprocess, error) {
	var dt Cmdprocess
	dt.ip = ip
	dt.port = port
	address := dt.ip + ":" + dt.port

	fmt.Printf("NewCMD: %s \n", address)
	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Error("CMDprocess Listen can not Setup\n")
		return nil, err
	}
	dt.ln = ln

	return &dt, nil
}

func (dt *Cmdprocess) CMDAccept(c *Canal) error {

	for {
		conn, err := dt.ln.Accept()
		if err != nil {
			continue
		}
		go dt.newCMDprocess(conn, c)
	}
}

func (dt *Cmdprocess) newCMDprocess(client net.Conn, c *Canal) error {
	defer client.Close()

	var authed bool

	for i := 0; ; i++ {
		client.Write([]byte(telnetCmdPrompt))
		inputCmd, inputArgv, err := netConnReadCmdArgv(client, 1000000)
		log.Infof("telnet input: %s: %v \n", inputCmd, inputArgv)
		if err != nil {
			log.Error(err)
			return nil
		}

		client.Write([]byte(CtlNewline + time.Now().String()))

		// 检查是否认证或此操作为认证
		if !authed && inputCmd != "auth" {
			client.Write([]byte(CtlNewline + fmt.Sprintf("Please auth first !")))
			client.Write([]byte(telnetCmdHelpStr))
			continue
		}

		if inputCmd == "info" {
			err = telnetinfocmd(c, client)
		} else if inputCmd == "auth" && len(inputArgv) == 1 {
			err = telnetauthcmd(c, client, inputArgv, &authed)
		} else if inputCmd == "exit" {
			client.Write([]byte("OK !! BYE BYE !!"))
			break
		} else if inputCmd == "loglevel" && len(inputArgv) == 1 {
			err = telnetloglevelcmd(c, client, inputArgv)
		} else if inputCmd == "help" {
			client.Write([]byte(telnetCmdHelpStr))
		} else if inputCmd == "off" && len(inputArgv) == 1 {
			err = telnetoffcmd(c, client, inputArgv)
		} else {
			client.Write([]byte(telnetCmdHelpStr))
		}

		// client.Write([]byte(CtlNewline + time.Now().String()))

		if err != nil {
			client.Write([]byte(fmt.Sprintf("CMD err: %s", err.Error())))
			return err
		}

	}

	return nil
}

func telnetoffcmd(c *Canal, client net.Conn, argv []string) error {
	if argv[0] == "clockdump" {
		if c.dumping {
			client.Write([]byte(CtlNewline + fmt.Sprintf("DATABUS is Dumping Clock Now ! off clockdump Wrong !")))
			return nil
		}
		c.dumping = true
		client.Write([]byte(CtlNewline + fmt.Sprintf("OK")))
		return nil
	}

	client.Write([]byte(telnetCmdHelpStr))
	return nil
}

func telnetauthcmd(c *Canal, client net.Conn, argv []string, authed *bool) error {
	if argv[0] == c.cfg.AUTH {
		*authed = true
		client.Write([]byte(CtlNewline + fmt.Sprintf("OK")))
		return nil
	}

	client.Write([]byte(CtlNewline + fmt.Sprintf("AUTH PASSWD WRONG !")))
	return nil
}

func telnetloglevelcmd(c *Canal, client net.Conn, argv []string) error {

	if argv[0] == "debug" {
		log.SetLevelByString("debug")
	} else if argv[0] == "info" {
		log.SetLevelByString("info")
	} else if argv[0] == "warning" {
		log.SetLevelByString("warning")
	} else if argv[0] == "error" {
		log.SetLevelByString("error")
	} else if argv[0] == "fatal" {
		log.SetLevelByString("fatal")
	} else {
		client.Write([]byte(telnetCmdHelpStr))
		return nil
	}

	client.Write([]byte(CtlNewline + fmt.Sprintf("OK")))
	return nil
}

func telnetinfocmd(c *Canal, client net.Conn) error {

	info := fmt.Sprint("DUMP MESSAGE --------------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("MASTER BINLOG FILE: %s ", c.master.Name)
	info = info + CtlNewline + fmt.Sprintf("MASTER POS: %d ", c.master.Position)
	info = info + CtlNewline + fmt.Sprintf("MASTER DUMPFILE %s ", c.master.Dumpfile)
	info = info + CtlNewline + fmt.Sprintf("MASTER DUMPBINLOG FILE %s ", c.master.DumpPosName)
	info = info + CtlNewline + fmt.Sprintf("MASTER DUMPPOS %d ", c.master.DumpPos)
	info = info + CtlNewline + fmt.Sprintf("KAFKA DUMP OFFSET %d ", c.master.Dumpoffset)
	info = info + CtlNewline + fmt.Sprint("DIR MESSAGE ---------------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("LogDir %s ", c.cfg.LogDir)
	info = info + CtlNewline + fmt.Sprintf("DataDir %s ", c.cfg.DataDir)
	info = info + CtlNewline + fmt.Sprint("LogLevel MESSAGE ----------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("LogLevel %s ", c.cfg.LogLevel)
	info = info + CtlNewline + fmt.Sprint("DB MESSAGE ----------------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("Master DB IP %s ", c.cfg.Addr)
	info = info + CtlNewline + fmt.Sprintf("DATABUS  ServerID %d ", c.cfg.ServerID)
	info = info + CtlNewline + fmt.Sprint("DUMPCLOCK MESSAGE ---------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("DumpClock Duration %d ", c.cfg.DumpClock)
	info = info + CtlNewline + fmt.Sprint("ZK & KAFKA MESSAGE --------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("Kafka Brokers %v ", c.cfg.Brokers)
	info = info + CtlNewline + fmt.Sprintf("ZkPath %v ", c.cfg.ZkPath)
	info = info + CtlNewline + fmt.Sprint("MONITOR MESSAGE ------------------------------------ ")
	info = info + CtlNewline + fmt.Sprintf("Dump Transfer Port %s ", c.cfg.MonitorPort)
	info = info + CtlNewline + fmt.Sprintf("Client Port %s ", c.cfg.MnotiroClientPort)
	info = info + CtlNewline + fmt.Sprint("MIRROR MESSAGE ------------------------------------- ")
	info = info + CtlNewline + fmt.Sprintf("Mirrorhost %s ", c.cfg.Sshhostname)
	info = info + CtlNewline + fmt.Sprintf("Mirrorenable %t ", c.cfg.Mirrorenable)

	//client.Write([]byte(CtlClearScreen))
	client.Write([]byte(CtlNewline + fmt.Sprintf(info)))
	return nil
}

func netConnReadCmdArgv(client net.Conn, timeout time.Duration) (string, []string, error) {
	client.SetReadDeadline(time.Now().Add(time.Second * timeout))

	data := make([]byte, 1024)
	n, err := client.Read(data)

	if e, ok := err.(*net.OpError); ok && e.Timeout() {
		return "", nil, nil
	}

	if err != nil || n < 0 || n >= 1024 {
		return "", nil, err
	}

	inputItems := make([]string, 0)
	strs := strings.Split(strings.Trim(string(data[0:n]), "\r\n"), " ")
	for _, str := range strs {
		if str != "" {
			inputItems = append(inputItems, str)
		}
	}

	if len(inputItems) == 0 {
		return "", nil, nil
	}

	if len(inputItems) == 1 {
		return inputItems[0], nil, nil
	}

	return inputItems[0], inputItems[1:], nil
}
