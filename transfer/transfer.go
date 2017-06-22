package transfer

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"time"

	"github.com/ngaut/log"
)

/*
type TransingChan struct {
	Intrans   chan string
	Exittrans chan string
}
*/

var FileTransing = make(map[string]bool)

type DataTrans struct {
	ln   net.Listener
	ip   string
	port string
}

func addFileTransing(file string) {
	FileTransing[file] = true
}

func delFileTransing(file string) {
	FileTransing[file] = false
}

func NewListener(ip string, port string) *DataTrans {
	var dt DataTrans
	dt.ip = ip
	dt.port = port
	address := dt.ip + ":" + dt.port

	ln, err := net.Listen("tcp", address)
	if err != nil {
		panic("Listen can not Setup")
	}
	dt.ln = ln

	return &dt
}

func (dt *DataTrans) Accept(dir string) error {

	for {
		conn, err := dt.ln.Accept()
		if err != nil {
			continue
		}
		go dt.newTransfer(conn, dir)
	}
}

func (dt *DataTrans) newTransfer(conn net.Conn, dir string) error {
	defer conn.Close()

	// connReader := bufio.NewReader(conn)

	connReader := bufio.NewReader(conn)

	firstline, err := connReader.ReadString('\n')
	if err != nil {
		log.Error("SERVER: GET FILENAME ERROR\n")
		error.Error(err)
		return err
	}

	filename := firstline[0 : len(firstline)-1]

	addFileTransing(filename)
	defer func() { delFileTransing(filename) }()

	_, err = conn.Write([]byte("PONG\n"))

	if !IsFileExists(path.Join(dir, filename)) {
		log.Error("SERVER: FILE %s IS NOT EXISTS! \n", filename)
		return nil
	}

	fmt.Printf("Get Filename From Net is %s \n", path.Join(dir, filename))

	fin, err := os.OpenFile(path.Join(dir, filename), os.O_RDWR, 0666)
	// fin, err := os.Open(path.Join(dir, filename))
	if err != nil {
		log.Error(err)
		return err
	}
	fininfo, _ := fin.Stat()
	log.Infof("SERVER: TRANSFER FILE STATE file[%s] size[%d]\n", fininfo.Name(), fininfo.Size())

	filerReader := bufio.NewReader(fin)

	log.Infof("Start Transfer File %s to Customer !!\n", filename)
	for {
		line, err := filerReader.ReadBytes('\n')
		if err == io.EOF {
			log.Info("SERVER: READ FILE OVER\n")
			log.Info("Transfer DumpFile Over,Close conn")
			time.Sleep(time.Second * 10)
			return nil
		} else if err != nil {
			log.Error("SERVER: READ FILE Error\n")
			return err
		}

		_, err = conn.Write(line)
		if err != nil {
			log.Error("SERVER: SEND BYTE ERROR\n")
		}
	}

	log.Infof("End Transfer File %s to Customer !!\n", filename)

	return nil

}

func IsFileExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if f.IsDir() {
		return false
	}

	return true
}
