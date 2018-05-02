# cdc
[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0.html)

mysql binlog parser json into rabbitmq or others, such as kafka
## What is cdc?
change data capture
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

Set the database configuration in etcd
```config
cdc/master/admin/config/app/mysql/host 10.0.0.2
cdc/master/admin/config/app/mysql/port 3306
cdc/master/admin/config/app/mysql/username admin
cdc/master/admin/config/app/mysql/password 123
```
start program
```
CdcClient client = new CdcClient(etcdHost, etcdUsername, etcdPassword);
client.setInstance("admin");
client.setWatchAllTable(true);
client.start();
```
## Documentation
### English
### 简体中文
## Architecture
## Connect with us
### Q&A
