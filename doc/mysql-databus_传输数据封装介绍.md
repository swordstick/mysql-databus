## MYSQL-DATABUS 传输数据封装介绍

### DUMP中的数据

* DUMP中保存的是JSON化的行操作数据

* ROWSEVENT

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

* 每个列的值在写入，DUMP SNAPSHOT文件，通过网络传输之前，都经过BASE64编码，并在CLIENT客户端解码

### DUMP中数据的传输

* 服务器端接收到客户端申请后，生产独立go routine处理
* 传输前，记录下该文件，标注为使用中，在其后的DUMP文件删除中屏蔽掉该文件
* 传输后，撤销使用标记
* 服务器端与客户端直接传输

### BINLOG数据

* BINLOG EVENT数据，在解析后，分为DDL和DML两类
* 数据类型中DML采用的是ROWSEVENT结构体，同DUMP
* 数据类型中DDL采用的是QueryEvent结构体

```
type QueryEvent struct {
	Table  *schema.Table
	Action string

	Query []byte
}

```
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


### BINLOG数据的传输

* 服务端与客户端无直接传输该类数据
* 服务端推送KAFKA，客户端根据OFFSET获取
* 数据在传入KAFKA之前，先进行BASE64编码，后进行JSON化
* 数据由SERVER端直接推送到KAFKA内，不予客户端直接通信
* KAFKA的对应KEY值，保存数据库的偏移量，即binlogfile 和 pos 位置 ，以及操作类型说明(DML && DDL)

```
type KafkaKey struct {
	Pos    mysql.Position
	Action string
}
```

* KAFKA的对应VALUE值，保存对应的数据类型值的JSON内容

```
type QueryEvent struct {
	Table  *schema.Table
	Action string

	Query []byte
}

```

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


