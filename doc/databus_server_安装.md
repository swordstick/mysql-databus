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
