## DATABUS SERVER 安装


### 编译源码

```
$ export $GOBIN={YOUR_GOBIN}
$ export $GOPATH={YOUR_GOPAH}
$ cp {源码} {GOPATH/src}
$ cd {YOUR_GOPATH}
$ GO INSTALL src/main/databus.go
```

## 用法

### 服务端启动方法

```
// -c不指定时，默认在本文件夹寻找 databus.toml
./databus -c {CONFIGFDIR}/databus.toml 
```

## 部署方式

### 单机部署

在配置文件中将mirror_enable设置为false即可，在单机启动程序

这类模式下

* 数据文件不会镜像到mirror机器(本来就没有)
* 也不会去注册ZK的有效节点
* 启动时也不会检查ZK的有效节点是否自身
* 启动时的偏移量依赖本地文件，而非ZK注册内容


### 容灾部署

* mirror_enable设置为true
* ssh相关参数都设置起来
* remote_dir也需要设置
* keepalivepath参数也需要设置
* remote_dir即是对端镜像机器的data_dir目录
* 两台机器镜像性的配置

设置以上参数后，2台机器中，先启动的程序抢先注册到ZK中，作为主节点，作为HA的方式

这类模式下：

* 启动后，如果第一次进行DUMP初始化落地，若无法备份dump文件至镜像节点，程序退出
* 定时的dump，若文件无法备份至镜像节点，程序不会退出，但会放弃本次DUMP文件
* DUMP成功容灾后，会发布相关信息到ZK，以实现DUMP更新的发布