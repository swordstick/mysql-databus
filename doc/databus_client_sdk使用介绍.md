## DATABUS CLIENT SDK使用介绍

### 引用包

* 包命名为 grandet ，【葛朗台】

### 示例程序说明

* 实例程序在main/grandet.go中

#### 初始化变量

```
	// 初始化配置信息
	var cfg grandet.Config
		cfg.Brokers = []string{"10.29.204.73:19092", "10.27.185.100:19092", "10.169.117.85:19092"}
	cfg.ZkPath = []string{"10.29.204.73:12181", "10.27.185.100:12181", "10.169.117.85:12181"}
	cfg.FirstLoad = true
	cfg.NodeName = "DumpMeta"
	cfg.Topic = "databustest.test"
	cfg.Serverip = "10.169.117.85"
	cfg.Serverport = "888"
	cfg.LogLevel = "debug"
	cfg.LogDir = "/root/log"
	cfg.LogFile = "grandet_error.log"
	cfg.Mirrorenable = true
	cfg.KeepAlivepath = "KeepAliveNode"
	cfg.Replinfopath = "/root/data/Repl.info"

```

#### 重点说明:

```
cfg.NodeName
cfg.Topic 
```
* 两者拼凑起来，作为ZK中的地址，获取相关DUMP 信息和KAFKA 的对应offset信息

```
cfg.KeepAlivepath
cfg.Topic 
```

* 两者軿凑起来，作为ZK中的地址，获取当前实际活跃的服务器端

```
cfg.Mirrorenable = true
```
* 若该值为false，代表客户端不会通过ZK来判断活跃节点，而是直接使用
```
	cfg.Serverip = "10.169.117.85"
	cfg.Serverport = "888"
```

```
cfg.Mirrorenable = false
```
* 若该值为false，代表客户端会通过ZK来判断活跃节点，以下两个参数失效
```
	cfg.Serverip = "10.169.117.85"
	cfg.Serverport = "888"
```


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


