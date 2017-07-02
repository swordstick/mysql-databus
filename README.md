# MYSQL-DATABUS简介

databuse是一个由GO开发,高可用的，Mysql数据快照和数据变更存储，分发的项目。<br>
其分为服务端和客户端SDK两部分，服务端存储DUMP快照及BINLOG实时变更，客户端SDK通过简单的API调用，即可从服务端获取到全量快照及其后的变更数据。<br>
整个Mysql数据抽取和分发，获取的细节，被封装起来，使用者只要关注数据的使用即可。<br>
该项目致力于简化DBA或者应用开发人员对来自Mysql数据的实时获取和变型使用。

* MYSQL-DATABUS暂未支持GTID,近期将更新支持<br>
<img src="http://orxb6fkuo.bkt.clouddn.com/%E6%B6%82%E9%B8%A6.jpg" width = "450" height = "300" alt="DATABUS"  />
> 图1来自网络，参考<br>


## 主要功能

1. 提供数据初始化快照获取能力(比图1更完善,重点功能)
2. 服务端支持容灾部署
3. 支持DUMP快照的定期更新，保证客户端追加变更的效率
4. 初始化结束后，客户端运行过程中若关闭，重启后自动续传实时变更
5. 客户端SDK自行解决数据重发问题，使用者不受干扰
6. 支持多客户端同时访问，随机访问服务端，期间不会对Mysql源产生任何压力
7. 支持客户端对服务端节点的自动分析，无需关注服务端的实际部署
8. 支持TEXT字段及字符串中保存的JSON值和特殊字符的正确传输，支持BLOB字段
9. 支持实时查阅服务端信息，动态修改服务端部分配置项
10. 提供大量配置项，避免对部署环境的限制
11. 提供使用简单的客户端SDK实时获取数据，数据消费者按自身需要灵活使用获取的数据
12. 服务端支持一个库指定表，指定多个库，指定全部库三种模式
13. 服务端支持过滤表
14. 提供SDK客户端指定起始BINLOG POS位置(绕过初始化)
15. 支持DDL的传输(包含ALTER,CREATE,DROP,TRUNCATE)，解决了ALTER RENAME问题
16. DDL操作传输前自动增加SCHEMA，Clinet SDK端获取后，若目标端为MYSQL，不必切换DB即可使用
17. DML操作在Client SDK获取后，自动增加SCHEMA，，若目标端为MYSQL，不必切换DB即可使用

### CLIENT

* 为便于使用，将CLIENT SDK代码独立出来，放于一下GITHUB中，移步获取
[The Client SDK for mysql-databus](https://github.com/swordstick/grandet)

## MYSQL-DATABUS文档

### MYSQL-DATABUS 安装及使用

1. [DATABUS SERVER安装][1]
2. [配置文件介绍][2]
3. [交互命令介绍][3]
4. [databus_client_sdk使用介绍-初始化][4]
5. [databus_client_sdk使用介绍-非初始化][11]

### MYSQL-DATABUS架构与设计

1. [MYSQL-DATABUS 架构介绍][5]
2. [MYSQL-DATABUS 服务端高可用实现][6]
3. [MYSQL-DATABUS 传输数据封装介绍][7]
4. [MYSQL-DATABUS SDK设计介绍][8]
5. [MYSQL-CLIENT GETEVNET()返回的数据结构][9]


### 鸣谢：

* 感谢[go-mysql][10]的作者siddontang，Mysql-Databus依赖的datapipe最初实现基于go-mysql工具包
* 感谢编写过程中提供各类思路的延允,盟主,邵青等好基友

### 作者：

swordstick<br>
[linkedin链接](http://www.linkedin.com/in/swordstick/)<br>
[技术站 www.dbathread.com][db]<br>

[db]: http://www.dbathread.com
[1]: https://github.com/swordstick/mysql-databus/blob/master/doc/databus_server_%E5%AE%89%E8%A3%85.md
[2]: https://github.com/swordstick/mysql-databus/blob/master/doc/%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6%E4%BB%8B%E7%BB%8D.md
[3]: https://github.com/swordstick/mysql-databus/blob/master/doc/%E4%BA%A4%E4%BA%92%E5%91%BD%E4%BB%A4%E4%BB%8B%E7%BB%8D.md
[4]: https://github.com/swordstick/mysql-databus/blob/master/doc/databus_client_sdk%E4%BD%BF%E7%94%A8%E4%BB%8B%E7%BB%8D-%E5%88%9D%E5%A7%8B%E5%8C%96.md
[5]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E6%9E%B6%E6%9E%84%E4%BB%8B%E7%BB%8D.md
[6]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E6%9C%8D%E5%8A%A1%E7%AB%AF%E9%AB%98%E5%8F%AF%E7%94%A8%E5%AE%9E%E7%8E%B0.md
[7]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_%E4%BC%A0%E8%BE%93%E6%95%B0%E6%8D%AE%E5%B0%81%E8%A3%85%E4%BB%8B%E7%BB%8D.md
[8]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-databus_sdk%E8%AE%BE%E8%AE%A1%E4%BB%8B%E7%BB%8D.md
[9]: https://github.com/swordstick/mysql-databus/blob/master/doc/mysql-client_getevnet%E5%87%BD%E6%95%B0%E8%BF%94%E5%9B%9E%E7%9A%84%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%84.md
[10]: https://github.com/siddontang/go-mysql
[11]: https://github.com/swordstick/mysql-databus/blob/master/doc/databus_client_sdk%E4%BD%BF%E7%94%A8%E4%BB%8B%E7%BB%8D-%E9%9D%9E%E5%88%9D%E5%A7%8B%E5%8C%96.md
[12]: http://orxb6fkuo.bkt.clouddn.com/%E6%B6%82%E9%B8%A6.jpg