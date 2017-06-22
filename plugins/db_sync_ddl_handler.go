package plugins

import (
	"canal"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/schema"
)

const ThreadforConn = 20

type DbSyncQueryHandler struct {
	lock  sync.Mutex
	dbase *sql.DB
}

func NewDbSyncQueryHandler(cfg *canal.Config) *DbSyncQueryHandler {
	var tranferUri string
	tranferUri = fmt.Sprintf("%s:%s@tcp(%s)/%s", cfg.Tuser, cfg.Tpassword, cfg.Taddr, cfg.Dump.TableDB)
	db, _ := sql.Open("mysql", tranferUri+canal.MysqlTimeout)
	err := db.Ping()
	if err != nil {
		panic(err.Error())
	}
	db.SetMaxOpenConns(ThreadforConn)
	db.SetMaxIdleConns(5)
	handler := &DbSyncQueryHandler{
		dbase: db}

	return handler
}

func (h *DbSyncQueryHandler) Close() {
	h.dbase.Close()
}

func (h *DbSyncQueryHandler) String() string {
	return "DbSyncQueryHandler"
}

func (h *DbSyncQueryHandler) Do(e *canal.QueryEvent) error {

	var err error

	if e.Action == canal.AlterAction {
		log.Infof("Alter table %s", e.Table.Name)
		err = h.alter(e.Query, e.Table)
	} else {
		return nil
	}

	if err != nil {
		log.Errorf("handle DDL err: %v", err)
		return canal.ErrHandleInterrupted
	}

	return nil
}

func (h *DbSyncQueryHandler) alter(Query []byte, table *schema.Table) error {

	sqlcmd := string(Query)
	if t, err := h.tableName(table.Schema, table.Name); err != nil {
		sqlcmd = strings.Replace(sqlcmd, table.Schema+"."+table.Name, t, -1)
	}
	_, err := h.dbase.Exec(sqlcmd)
	log.Infof("Exec sql: %s, err: %v", sqlcmd, err)
	if err != nil {
		return fmt.Errorf("Exec sql(%s) Failed, err: %v", sqlcmd, err)
	}

	return nil
}

func (h *DbSyncQueryHandler) tableName(srcSchema string, srcName string) (string, error) {

	t, ok := canal.Cfg_Tc[srcName]

	if ok {
		return fmt.Sprintf("%s.%s", srcSchema, t), errors.Errorf("DDL Table changed to %s ", t)
	} else {
		return fmt.Sprintf("%s.%s", srcSchema, srcName), nil
	}
}
