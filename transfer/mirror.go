package transfer

import (
	"fmt"
	"net"
	"os"
	"path"
	"time"

	"github.com/ngaut/log"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Ssh_mirror struct {
	Sshhostname string
	Sshport     string
	Sshuser     string
	Sshpasswd   string
	sftpClient  *sftp.Client
}

func (s *Ssh_mirror) CloseSftp() {
	s.sftpClient.Close()
}

func NewSftp(Sshhostname string, Sshport string, Sshuser string, Sshpasswd string) (*Ssh_mirror, error) {

	mirror := &Ssh_mirror{
		Sshhostname: Sshhostname,
		Sshpasswd:   Sshpasswd,
		Sshuser:     Sshuser,
		Sshport:     Sshport,
	}

	var (
		auth         []ssh.AuthMethod
		addr         string
		clientConfig *ssh.ClientConfig
		sshClient    *ssh.Client
		sftpClient   *sftp.Client
		err          error
	)

	// get auth method
	auth = make([]ssh.AuthMethod, 0)
	auth = append(auth, ssh.Password(Sshpasswd))
	clientConfig = &ssh.ClientConfig{
		User:    Sshuser,
		Auth:    auth,
		Timeout: 30 * time.Second,
		HostKeyCallback: func(Sshhostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}
	// connet to ssh
	addr = fmt.Sprintf("%s:%s", Sshhostname, Sshport)
	if sshClient, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		return &Ssh_mirror{}, err
	}
	// create sftp client
	if sftpClient, err = sftp.NewClient(sshClient); err != nil {
		return &Ssh_mirror{}, err
	}

	mirror.sftpClient = sftpClient

	return mirror, nil
}

func (s *Ssh_mirror) TransOrNot(localFilePath, remoteDir string) (bool, error) {

	// 用来测试的本地文件路径 和 远程机器上的文件夹

	srcFile, err := os.Open(localFilePath)
	if err != nil {
		return false, err
	}
	defer srcFile.Close()
	var remoteFileName = path.Base(localFilePath)

	// 检查文件是否已经存在

	dstdir, err := s.sftpClient.ReadDir(remoteDir)
	if err != nil {
		return false, err
	}

	var remotefileexist bool

	for _, dstfile := range dstdir {
		if dstfile.Name() == remoteFileName && !dstfile.IsDir() {
			remotefileexist = true
			log.Infof("dstfile name: %s", dstfile.Name())
			log.Infof("remoteFileName: %s", remoteFileName)
		}
	}

	// 检查文件是否一样大小

	if remotefileexist {
		dstinfo, err := s.sftpClient.Stat(path.Join(remoteDir, remoteFileName))
		if err != nil {
			return false, err
		}

		srcinfo, err := os.Stat(localFilePath)
		if err != nil {
			return false, err
		}

		log.Infof("dstinfo size is %d", dstinfo.Size())
		log.Infof("srcinfo size is %d", srcinfo.Size())

		if srcinfo.Size() == dstinfo.Size() {
			log.Infof("src and dst have same size")
			return false, nil
		}
	}

	return true, nil
}

func (s *Ssh_mirror) TransFile(localFilePath, remoteDir string) (bool, error) {

	var remoteFileName = path.Base(localFilePath)

	srcFile, err := os.Open(localFilePath)
	if err != nil {
		return false, err
	}
	defer srcFile.Close()

	dstFile, err := s.sftpClient.Create(path.Join(remoteDir, remoteFileName))
	if err != nil {
		return false, err
	}
	defer dstFile.Close()

	buf := make([]byte, 1024)
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		dstFile.Write(buf)
	}
	log.Info("copy file to remote server finished!")

	return true, nil
}
