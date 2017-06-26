## DATABUS CLIENT SDK使用介绍-非初始化

### 引用包

* 包命名为 grandet ，【葛朗台】

### 示例程序说明

* 初始化
* 实例程序在main/grandet_WithPos.go中
* FirsetLoad = true && Replinfopath文件不存在
* 配置了	cfg.MasterName &&	cfg.MasterPos

#### 初始化变量

```
	// 初始化配置信息
	cfg.Brokers = []string{"10.29.1.1:19092", "10.27.1.2:19092", "10.169.1.3:19092"}
	cfg.ZkPath = []string{"10.29.1.1:12181", "10.27.1.2:12181", "10.169.1.3:12181"}
	cfg.FirstLoad = false
	cfg.Topic = "databustest.test"
	cfg.LogLevel = "debug"
	cfg.LogDir = "/root/log"
	cfg.LogFile = "grandet_error.log"
	// 若不配置，默认为当前文件夹
	cfg.Replinfopath = "/root/data/Repl.info"
	cfg.MasterName = "binlog.000145"
	cfg.MasterPos = 2165057		
```

#### 重点说明:

	cfg.MasterName = "binlog.000145"
	cfg.MasterPos = 2165057
	cfg.Topic = "databustest.test"

从Kafka换算Offset，做出同步初始位置

### 启动同步

```
	// 新建Client
	Cl, err := grandet.NewClient(&cfg)
	if err != nil {
		panic(err)
	}
	// Client启动同步
	Cl.ClientStart()

```

### 变更获取函数

```
			ev, err := Cl.GetEvent(ctx)

```

### 返回的数据结构类型判断

```
			switch e := ev.Event.(type) {
			case grandet.RowsEvent:
			...
			case grandet.QueryEvent:
			...
```

* grandet.RowsEvent表示为DML操作
* grandet.QueryEvent表示为DDL操作


### 数据结构拼装成SQL

* 函数调用

```
				cmd := grandet.Do(&e)

```

Do定义在grandet/aide_event.go文件中
作为参考提供


