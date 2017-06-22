## MYSQL-DATABUS 服务端高可用实现


### 架构图
![架构图片](http://orxb6fkuo.bkt.clouddn.com/Snip20170621_5.png)


### 高可用的设计


#### 高可用基本模式


* 使用MIRROR方式实现HA模式

#### 核心数据的备份

* DUMP文件本身
* 当前解析的binlog偏移位置
* DUMP文件信息，及当时的binlog偏移位置

#### DUMP文件生产和高可用

* mysqldump导出的文件，经过解析，最终以JSON LIST保存到本地文件，它用于初始化。
* 除第一次DATABUSE SERVER启动，由主线程完成落地外，其他定时任务由GO ROUTINE完成。
* 完成DUMP SNAPSHOT落地后，需要先行同步到镜像节点的指定位置，才会予以公开，否则宁可放弃本次落地
* 若第一次落地无法完成同步镜像节点，SERVER端报错退出
* 同步完成后，向ZK变更当前DUMP的信息，用于其后的CLIENT端使用
* 原CLIENT对旧DUMP文件的数据获取不会受到影响
* DUMP文件列表，保存在DATADIR的dump.index中，定时删除旧文件，默认保留3份
* 定时任务只会删除当前未被使用的文件

#### binlog偏移位置

* 考虑到同步状态信息修改频繁，不会在MIRROR间备份
* 每次event操作，在推送到KAFKA之际，加入对应的额外MYSQL POS信息
* 发生切换时，从KAFKA中分析当前POS信息

#### DUMP相关信息

* 存储在ZK中

#### SERVER程序的高可用

* 备节点启动后，通过KAFKA的数据分析获取到当前MYSQL的解析偏移位置
* 主节点单独协程定时向ZK注册
* 备节点循环检查ZK的活跃节点信息，当主节点在超时时间后尚未再次更新ZK，备节点将进入替换流程
* 主节点因网络等原因，与ZK失联后，再次返回需要10分钟等待后，方可进行ZK访问
* 主节点在向ZK注册时，需要检查当前活跃节点是否被修改，避免脑裂的出现

#### CLIENT侧

* Client启动后，首先从ZK中获取如下信息

| 信息 | 用途 | 位置 | 
| -- | -- | -- | 
| 当前活跃节点 | 用于其后通信获取DUMP内容 | /initpath/NodeName/Topic | 
| 当前DUMP信息 | 用于其后指定需要的DUMP内容，及随后在KAFKA的OFFSET位置 | /initpath/KeepAlivepath/Topic | 

* 随后与SERVER端通信，在线流式获取DUMP中的DML数据，在BASE64解码后，封装为结构体返回给客户端
* 完成DUMP接受，开始从KAFKA指定OFFSET获取EVENT，在BASE64解码后，封装为结构体返回给客户端

#### 重要补充


* 获取的BINLOG中包含DDL操作，目前只支持ALTER
* DUMP的获取仅包含DML操作，如果你希望最终推送给MYSQL，请自行建立需要的表结构








