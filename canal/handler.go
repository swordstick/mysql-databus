package canal

import (
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	ErrHandleInterrupted = errors.New("do handler error, interrupted")
)

// add for all QueryEvent
/*
type QueryEventHandler interface {
	// Handle RowsEvent, if return ErrHandleInterrupted, canal will
	// stop the sync
	Do(e *QueryEvent) error
	String() string
	Close()
}
*/

type QueryEventHandler interface {
	// Handle RowsEvent, if return ErrHandleInterrupted, canal will
	// stop the sync
	Do(e interface{}, action string) error
	String() string
	Close()
}

func (c *Canal) RegQueryEventHandler(h QueryEventHandler) {
	c.rsLock.Lock()
	c.quHandlers = append(c.quHandlers, h)
	c.rsLock.Unlock()
}

func (c *Canal) travelQueryEventHandler(e *QueryEvent) error {
	c.rsLock.Lock()
	defer c.rsLock.Unlock()

	var tableExist bool
	tableExist = true

	for _, tb := range c.cfg.Dump.IgnoreTables {
		if tb == e.Table.Name {
			tableExist = false
		}
	}

	if !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	tableExist = false

	if len(c.cfg.Dump.Tables) == 0 {
		tableExist = true
	} else {
		for _, tb := range c.cfg.Dump.Tables {
			if tb == e.Table.Name {
				tableExist = true
			}
		}
	}

	if e.Table.Schema != c.cfg.Dump.TableDB || !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...\n",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	var err error
	for _, h := range c.quHandlers {
		if err = h.Do(e, "DDL"); err != nil && !mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("handle %v err: %v\n", h, err)
		} else if mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("handle %v err, interrupted\n", h)
			return ErrHandleInterrupted
		}
	}

	log.Debug("Push Message to Kafka with DDL. \n")
	return nil
}

type RowsEventHandler interface {
	// Handle RowsEvent, if return ErrHandleInterrupted, canal will
	// stop the sync
	Do(e interface{}, action string) error
	String() string
	Close()
}

func (c *Canal) RegRowsEventHandler(h RowsEventHandler) {
	c.rsLock.Lock()
	c.rsHandlers = append(c.rsHandlers, h)
	c.rsLock.Unlock()
}

func (c *Canal) travelRowsEventHandler(e *RowsEvent) error {
	c.rsLock.Lock()
	defer c.rsLock.Unlock()

	var tableExist bool
	tableExist = true

	for _, tb := range c.cfg.Dump.IgnoreTables {
		if tb == e.Table.Name {
			tableExist = false
		}
	}

	if !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	tableExist = false

	if len(c.cfg.Dump.Tables) == 0 {
		tableExist = true
	} else {
		for _, tb := range c.cfg.Dump.Tables {
			if tb == e.Table.Name {
				tableExist = true
			}
		}
	}

	if e.Table.Schema != c.cfg.Dump.TableDB || !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	var err error
	for _, h := range c.rsHandlers {
		if err = h.Do(e, "DML"); err != nil && !mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("Handle %v err: %v", h, err)
		} else if mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("Handle %v err, interrupted", h)
			return ErrHandleInterrupted
		}
	}

	log.Debug("Push Message to Kafka with DML. \n")

	return nil
}

type RowsEventDumpHandler interface {
	// Handle RowsEvent, if return ErrHandleInterrupted, canal will
	// stop the sync
	Do(e *RowsEvent) error
	String() string
	Close()
}

func (c *Canal) RegRowsEventDumpHandler(h RowsEventDumpHandler) {
	c.rsLock.Lock()
	// 多次注册会导致异常，所有DUMP过的文件都会反复执行写入
	// 如果两次注册同一个文件句柄，可能导致1个文件写入2份相同内容
	c.dpHandlers = []RowsEventDumpHandler{h}
	c.rsLock.Unlock()
}

func (c *Canal) travelRowsEventDumpHandler(e *RowsEvent) error {
	c.rsLock.Lock()
	defer c.rsLock.Unlock()

	var tableExist bool
	tableExist = true

	for _, tb := range c.cfg.Dump.IgnoreTables {
		if tb == e.Table.Name {
			tableExist = false
		}
	}

	if !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	tableExist = false

	if len(c.cfg.Dump.Tables) == 0 {
		tableExist = true
	} else {
		for _, tb := range c.cfg.Dump.Tables {
			if tb == e.Table.Name {
				tableExist = true
			}
		}
	}

	if e.Table.Schema != c.cfg.Dump.TableDB || !tableExist {
		log.Debugf("table name(%s.%s) not match ignore...",
			e.Table.Schema, e.Table.Name)
		return nil
	}

	var err error
	for _, h := range c.dpHandlers {
		if err = h.Do(e); err != nil && !mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("handle %v err: %v", h, err)
		} else if mysql.ErrorEqual(err, ErrHandleInterrupted) {
			log.Errorf("handle %v err, interrupted", h)
			return ErrHandleInterrupted
		}

	}
	return nil
}

// []byte, int64, float64, bool, string
func InterfaceToString(s interface{}) string {
	// Handle the most common destination types using type switches and
	// fall back to reflection for all other types.
	switch s := s.(type) {
	case nil:
		return "NULL"
	case string:
		return s
	case []byte:
		return string(s)
	case bool:
		return strconv.FormatBool(s)
	case int:
		return strconv.FormatInt(int64(s), 10)
	case int8:
		return strconv.FormatInt(int64(s), 10)
	case uint8:
		return strconv.FormatUint(uint64(s), 10)
	case int16:
		return strconv.FormatInt(int64(s), 10)
	case uint16:
		return strconv.FormatUint(uint64(s), 10)
	case int32:
		return strconv.FormatInt(int64(s), 10)
	case uint32:
		return strconv.FormatUint(uint64(s), 10)
	case int64:
		return strconv.FormatInt(int64(s), 10)
	case uint64:
		return strconv.FormatUint(uint64(s), 10)
	case float32:
		return strconv.FormatFloat(float64(s), 'f', 4, 32)
	case float64:
		return strconv.FormatFloat(s, 'f', 4, 64)
	case time.Time:
		return s.Format(mysql.TimeFormat)
	}

	return "nil"
}
