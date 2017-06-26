package canal

import (
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type DumpConfig struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `toml:"mysqldump"`

	// Will override Databases, tables is in database table_db
	Tables  []string `toml:"tables"`
	TableDB string   `toml:"table_db"`

	// fordbs
	Databases []string `toml:"dbs"`

	// Ignore table format is db.table
	IgnoreTables []string `toml:"ignore_tables"`

	// If true, discard error msg, else, output to stderr
	DiscardErr bool `toml:"discard_err"`
}

type Config struct {
	Addr              string     `toml:"addr"`
	User              string     `toml:"user"`
	Password          string     `toml:"password"`
	ServerID          uint32     `toml:"server_id"`
	Flavor            string     `toml:"flavor"`
	DataDir           string     `toml:"data_dir"`
	Topic             string     `toml:"topic"`
	dumpThreads       uint32     `toml:"dumpthreads"`
	DumpClock         int64      `toml:"dumpclock"`
	Brokers           []string   `toml:"brokers"`
	ZkPath            []string   `toml:"zkpath"`
	NodeName          string     `toml:"nodename"`
	Dump              DumpConfig `toml:"dump"`
	MonitorIP         string     `toml:"monitorip"`
	MonitorPort       string     `toml:"monitorport"`
	MnotiroClientPort string     `toml:"monitorclient"`
	LogFile           string     `toml:"logfile"`
	LogLevel          string     `toml:"log_level"`
	Sshhostname       string     `toml:"sshhostname"`
	Sshport           string     `toml:"sshport"`
	Sshuser           string     `toml:"sshuser"`
	Sshpasswd         string     `toml:"sshpasswd"`
	RemoteDir         string     `toml:"remote_dir"`
	Mirrorenable      bool       `toml:"mirror_enable"`
	LogDir            string     `toml:"log_dir"`
	KeepAlivepath     string     `toml:"keepalivepath"`
	Alivetimeout      int64      `toml:"masteralive_timeout"`
	AUTH              string     `toml:"auth"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

/*
func NewDefaultConfig() *Config {
	c := new(Config)

	c.Addr = "127.0.0.1:3306"
	c.User = "root"
	c.Password = ""

	rand.Seed(time.Now().Unix())
	c.ServerID = uint32(rand.Intn(1000)) + 1001

	c.Flavor = "mysql"
	c.DataDir = "./var"
	c.Dump.ExecutionPath = "mysqldump"
	c.Dump.DiscardErr = true

	return c
}
*/

func PathExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
