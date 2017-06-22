package plugins

import (
	"canal"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/ngaut/log"
)

type DataFileHandler struct {
	lf   *json.Encoder
	file *os.File
}

func NewDataFileHandler(cfg *canal.Config, dumpfile string) *DataFileHandler {

	// 此处若*DataFileHandler声明，会遇到"panic: runtime error: invalid memory address or nil pointer dereference"报错
	// 因ld为nil值，修改为如下方式即可
	ld := &DataFileHandler{nil, nil}

	var filer *os.File
	var err error
	pathf := path.Join(cfg.DataDir, dumpfile)
	filer, err = os.Create(pathf)
	log.Infof("Create Dump file %s", pathf)
	if err != nil {
		fmt.Println("FILE CREATE NIL")
		return nil
	}

	ld.lf = json.NewEncoder(filer)
	ld.file = filer
	// ld.lf = lf

	return ld
}

func (h *DataFileHandler) Close() {
	h.file.Close()
}

func (h *DataFileHandler) String() string {
	return "DataFileHandler"
}

func (h *DataFileHandler) Do(e *canal.RowsEvent) error {

	e = canal.Base64RowsEvent(e)
	if err := h.lf.Encode(e); err != nil {
		panic(err.Error())
	}

	return nil
}
