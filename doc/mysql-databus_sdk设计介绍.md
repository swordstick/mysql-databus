## MYSQL-DATABUS SDK设计介绍

### CLIENT

* Client启动后，首先从ZK中获取如下信息

| 信息 | 用途 | 位置 | 
| -- | -- | -- | 
| 当前活跃节点 | 用于其后通信获取DUMP内容 | /initpath/NodeName/Topic | 
| 当前DUMP信息 | 用于其后指定需要的DUMP内容，及随后在KAFKA的OFFSET位置 | /initpath/KeepAlivepath/Topic | 

* 随后与SERVER端通信，在线流式获取DUMP中的DML数据，在BASE64解码后，封装为结构体返回给客户端
* 完成DUMP接受，开始从KAFKA指定OFFSET获取EVENT，在BASE64解码后，封装为结构体返回给客户端
* SDK提供GETEVENT函数，该函数实现，通道接收实际的处理代码返回的数据类型，通道接收数据后返回给外层调用者
* GETEVENT函数，因此是一个等待函数


### 重要补充


* 获取的BINLOG中包含DDL操作，目前只支持ALTER
* DUMP的获取仅包含DML操作，如果你希望最终推送给MYSQL，请自行建立需要的表结构


