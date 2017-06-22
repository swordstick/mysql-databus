## MYSQL-CLIENT GETEVNET()返回的数据结构


### ROWSEVENT

```
type RowsEvent struct {
	Table  *schema.Table
	Action string
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
}
```


### QUERYEVENT



```
type QueryEvent struct {
	Table  *schema.Table
	Action string

	Query []byte
}

```

### SCHEMA.TABLE 结构体

* schema包

```
type Table struct {
	Schema string
	Name   string

	Columns   []TableColumn
	Indexes   []*Index
	PKColumns []int
}
```


```
type TableColumn struct {
	Name       string
	Type       int
	RawType    string
	IsAuto     bool
	EnumValues []string
	SetValues  []string
}

type Index struct {
	Name        string
	Columns     []string
	Cardinality []uint64
}

```


### 使用范例

```
func Do(e *RowsEvent) string {

	var cmd string

	for i := 0; i < len(e.Rows); i++ {
		var err error

		if e.Action == InsertAction {
			cmd, err = insert(e.Table, e.Rows[i])
		} else if e.Action == UpdateAction {
			cmd, err = update(e.Table, e.Rows[i], e.Rows[i+1])
			i += 1
		} else if e.Action == DeleteAction {
			cmd, err = delete(e.Table, e.Rows[i])
		} else {
			return "Do: Have No Match Action \n"
		}

		if err != nil {
			log.Errorf("handle data err: %v", err)
			return "Do: Have No Statement \n"
		}
	}

	return cmd
}

func insert(table *schema.Table, row []interface{}) (string, error) {
	var columns, values string
	for k := 0; k < len(table.Columns); k++ {
		columns += "`" + table.Columns[k].Name + "`,"
		if row[k] == nil {
			values += "NULL,"
		} else {
			values += "'" + EscapeStringBackslash(InterfaceToString(row[k])) + "',"
		}
	}
	if columns == "" || values == "" {
		log.Infof("insert is empty: %s %s", columns, values)
		return "", nil
	}
	columns = columns[0 : len(columns)-1]
	values = values[0 : len(values)-1]

	sqlcmd := "REPLACE INTO `" + table.Name + "` (" + columns + ") VALUES (" + values + ")"

	return sqlcmd, nil
}
```
