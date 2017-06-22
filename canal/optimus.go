package canal

import (
	"log"

	"github.com/BurntSushi/toml"
)

type ss struct {
	Source string
	Target string
}

type songs struct {
	Optimu []ss
}

var Cfg_Tc map[string]string

type sss struct {
	Table   string
	Columns []string
}

type songss struct {
	Filter []sss
}

type Cols map[string]bool

var FilterCols map[string]Cols
var FilterTabs map[string]bool

func ParseOptimus(filename string) error {

	Cfg_Tc = make(map[string]string)
	var favorites songs
	if _, err := toml.DecodeFile(filename, &favorites); err != nil {
		return err
	}

	for _, s := range favorites.Optimu {
		//fmt.Printf("%s (%s)\n", s.Source, s.Target)
		Cfg_Tc[s.Source] = s.Target
		//fmt.Printf("%s (%s)\n", s.Source, Cfg_Tc[s.Source])
	}

	return nil
}

func ParseFilter(filename string) error {

	FilterCols = make(map[string]Cols)
	FilterTabs = make(map[string]bool)

	var favorites songss

	if _, err := toml.DecodeFile(filename, &favorites); err != nil {
		log.Fatal(err)
	}

	for _, s := range favorites.Filter {
		var ColsTemp = make(Cols)
		for _, k := range s.Columns {
			ColsTemp[k] = true
		}
		FilterCols[s.Table] = ColsTemp
		FilterTabs[s.Table] = true
	}

	return nil
}
