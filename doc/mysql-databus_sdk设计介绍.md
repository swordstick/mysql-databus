## MYSQL-DATABUS SDK设计介绍


### 启动状态分析


| 归类 | 参数配置及文件状态 | 启动方式 |
| -- |-- | -- |
| 非初始化 | Replinfopath偏移文件<br>FirstLoad 随便<br> MasterName && MasterPos 随便 | 当该Replinfopath偏移文件已经存在<br>启动状态只能是非初始化，并且Kafka的Offset只能从这里获取<br>其他参数立即失效<br>设计原因：客户端重新启动只能让其续传，若想放弃续传，只能删除偏移文件|
| 非初始化 | FirstLoad = false <br> MasterName && MasterPos 配置合法<br>Replinfopath偏移文件 | Replinfopath偏移文件配置但不存在<br>非初始化启动<br>利用偏移位置MasterName MasterPos 分别代表的binlogfile binlogpos去Kafka换算 |
| 初始化 | FirstLoad = true <br> MasterName && MasterPos 随便 <br>Replinfopath偏移文件 | Replinfopath偏移文件配置但不存在<br> FirstLoad = true<br> 初始化启动，MasterName MasterPos 自动失效 ，启动后会完成Dump获取，继而转为偏移数据获取，对SDK的使用者GETEVENT只是增加了返回的DUMP操作而已,并无特别不一致 |



### CLIENT

* Client启动后，首先获取如下信息

| 信息 | 用途 | 位置 |
| -- | -- | -- |
| 初始化时,<br>当前活跃节点 | 用于其后通信获取DUMP内容 | ZK : /initpath/KeepAlivepath/Topic |
| 当前DUMP信息 | 用于其后指定需要的DUMP内容，及随后在KAFKA的OFFSET位置 | ZK : /initpath/NodeName/Topic |
| 非初始化时,<br>读取配置Replinfopath路径的偏移文件,去读Offset<br>或者按照启动代码指定的MasterName&&MasterPos换算Offset | 用于向Kafka获取数据 | Replinfopath: <br> MasterName && MasterPos |

#### 初始化

* 随后与SERVER端通信，在线流式获取DUMP中的DML数据，在BASE64解码后，封装为结构体返回给客户端
* 完成DUMP接受，开始从KAFKA指定OFFSET获取EVENT，在BASE64解码后，封装为结构体返回给客户端
* SDK提供GETEVENT函数，该函数实现，通道接收实际的处理代码返回的数据类型，通道接收数据后返回给外层调用者
* GETEVENT函数，因此是一个阻塞函数

#### 非初始化

* 根据偏移文件Replinfopath获取Offset，或者利用配置的MasterName && MasterPos，换算Offset
* SDK提供GETEVENT函数，该函数实现，通道接收实际的处理代码返回的数据类型，通道接收数据后返回给外层调用者
* GETEVENT函数，因此是一个阻塞函数


### 重要补充


* 获取的BINLOG中包含DDL操作和DML两类数据结构,详情参考 [MYSQL-CLIENT GETEVNET()返回的数据结构][9]
> DDL中Query即是可以直接执行的DDL语句，包含CREATE,ALTER,DROP,TRUNCATE<br>
> DML中并无直接的输出语句，为了便于理解其数据结构，在grandet/aide_event.go中，Do(rowsEvent)函数演示了，从结构体到操作语句的方法
* DUMP的获取仅包含DML操作，如果你希望最终推送给MYSQL，请自行建立需要的表结构，非DUMP情况下，建表语句会通过推送到Kafka,不必额外处理


* "Overflow Offset": 若给定的MasterName MasterPos来自Mysql的show master status ,并且Mysql此后并未有任何操作，那么启动2将会失败，并报错"Overflow Offset",这是因为Mysql的show master status 返回的值，指的是下一个操作的具体事务位置，尚未实际发生，而Kafka中只保存已经发生的事务，所以在Kafka中无从识别。解决办法：只要在Mysql侧随便做个操作即可。
* "Overflow Offset": 若给定早于服务端最近Dump位置的binlogNmae和binlogpos，可能在Kafka中已经没有过期，最后也会报此错误退出


[9]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-client_getevnet%E5%87%BD%E6%95%B0%E8%BF%94%E5%9B%9E%E7%9A%84%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%84.md