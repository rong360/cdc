# cdc
[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0.html)

mysql binlog parser json into rabbitmq or others(such as kafka)
## What is cdc?
change data capture, Key Features:
- High availability, cluster deployment
- Table level filter binlog file
- Supports all mysql field parsing
- Automatically save the binlog location node, smooth upgrade and restart, data is not lost
- Configure centralized management
- Operational status monitoring
- Dynamically loading table configuration files
- Support for adding multiple exported data sources for easy expansion to kafka or other
## Java Versions

Java 8 or above is required.

## Download

### Maven
```xml
<dependency>
  <groupId>com.rong360</groupId>
  <artifactId>cdc</artifactId>
  <version>1.0.0</version>
</dependency>
```
## Quick start
* install [etcd](https://coreos.com/etcd/docs/latest/dl_build.html), start etcd
```bash
$ ./bin/etcd
```
* Set the database configuration in etcd
```config
etcdctl put cdc/master/admin/config/app/mysql/host 10.0.0.2
etcdctl put cdc/master/admin/config/app/mysql/port 3306
etcdctl put cdc/master/admin/config/app/mysql/username admin
etcdctl put cdc/master/admin/config/app/mysql/password 123
```
* Start program
```java
CdcClient client = new CdcClient("http://127.0.0.1:2379", "", "");
client.setInstance("admin");
client.setWatchAllTable(true);
client.start();
```
## Documentation
### English
See [DOUCUMENT](https://github.com/rong360/cdc/blob/master/doc/english.md) for details.
### 简体中文
详细[文档](https://github.com/rong360/cdc/blob/master/doc/中文.md)。
## Architecture
## Connect with us
<zhangtao@rong360.com>

<liuchi@rong360.com>
### Q&A
