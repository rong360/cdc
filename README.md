# cdc
[![Build Status](https://img.shields.io/travis/rong360/cdc/master.svg?style=flat-square)](https://www.travis-ci.org/rong360/cdc)
[![License](https://img.shields.io/badge/Licence-Apache%202.0-orange.svg?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/com.rong360/cdc.svg?style=flat-square)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.rong360%22%20AND%20a%3A%22cdc%22)

A mysql binlog parser, which has running stable for more then 36 months in our production environment on more than 10 mysql instances. It transforms binlog event(insert/delete/update) into json format data into rabbitmq or other mq(such as kafka), Based on this project:https://github.com/shyiko/mysql-binlog-connector-java.
## What is cdc?
change data capture, Key Features:
- High availability, cluster deployment
- Table level filter
- Supports almost all mysql field parsing
- Automatically save  binlog file/position, smooth upgrade and restart
- Configure centralized management
- Operational status monitoring
- Dynamically loading table configuration files
- support split table config
- Mysql binlog data order Guaranteed 
- Support a easy way to tap it into other mq (sucn as kafka,nsq,redis...)
## dependency

- Java 8 or above is required.
- etcd 3.0 or above is required.
- mysql 5.6+

## Download

### Maven
```xml
<dependency>
  <groupId>com.rong360</groupId>
  <artifactId>cdc</artifactId>
  <version>1.0.1</version>
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
请见详细[文档](https://github.com/rong360/cdc/blob/master/doc/中文.md)。
## Architecture
## Connect with us
<zhangtao@rong360.com>

<liuchi@rong360.com>
### Q&A
