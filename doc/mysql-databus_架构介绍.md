## MYSQL-DATABUS 架构介绍


### 架构图
![架构图片](http://orxb6fkuo.bkt.clouddn.com/Snip20170621_5.png)

### 各部分作用

#### DATABUS SERVER

* 伪装为从库，从MYSQL中获取BINLOG信息
* 获取的BINLOG信息，解析后特定的数据结构，经过BASE64处理，再封装为JSON，推送到KAFKA中
* 使用MYSQLDUMP从MYSQL，获取DUMP FILE
* DUMP 的SQL解析后，以JSON list方式保存到文件中，并放置于本地，此处命名为DUMP SNAPSHOT
* DUMP SNAPSHOT信息，以及其对应的MYSQL偏移量，KAFKA的OFFSET，作为DUMP相关信息，发布到ZOOKEEPER中

#### ZOOKEEPER

* 用于保存DUMP的相关信息
* 用于当前服务端活跃注册及活跃节点探测
* 用于KAFKA

#### KAFKA 


* 用于保存偏移量，操作类型，和BINGLO解析的操作数据封装的数据结构

#### DATABUS CLIENT

* 通过ZK判断活跃节点
* 通过ZK判断DUMP信息及对应的KAFKA的OFFSET
* 通过与活跃节点的通信获取DUMP数据
* 通过与KAFKA的消费，获取binlog数据
* 通过统一的函数调用返回获取的DUMP数据和BINLOG数据

#### YOUR PROCESS

* 使用者自行编写的，用于二次处理数据的代码
* 比如可以用于数据重组为KV，推送到REDIS,ES,HBASE等
* 比如可以用于重组后，拆分为多个表，或者多个表合并为单表，字段变形等，最后依然推送到MYSQL