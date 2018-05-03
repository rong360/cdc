CDC documentation
=================

Introduce
---------

MYSQL BINLOGS TO JSON conversion tool. Through the dump mysql binlogs file, the
parsed data is converted into a JSON format for data synchronization or data
change monitoring

Features
--------

-   High availability, cluster deployment

-   Table level filter binlog file

-   Supports all mysql field parsing

-   Automatically save the binlog location node, smooth upgrade and restart,
    data is not lost

-   Configure centralized management

-   Operational status monitoring

-   Dynamically loading table configuration

-   Support for adding multiple exported data sources for easy expansion to
    kafka or other

Design implementation
---------------------

### Binlog dump technical solution selection

| Repo                                                                                  | Implementation language | Instructions                                                                                    |
|---------------------------------------------------------------------------------------|-------------------------|-------------------------------------------------------------------------------------------------|
| [python-mysql-replication](https://github.com/noplay/python-mysql-replicationication) | Python                  | Pass, not familiar with python                                                                  |
| [php-mysql-replication](https://github.com/krowinski/php-mysql-replication)           | PHP                     | Pass, lack of high availability, multithreading                                                 |
| [mysql-binlog-connector-java](https://github.com/shyiko/mysql-binlog-connector-java)  | Java                    | Choosed, pure java development, does not rely on third-party jar packages, features lightweight |
| [canal](https://github.com/alibaba/canal)                                             | Java                    | Pass, Ali's open source solution, heavyweight, local stable reproduction bug                    |
| [go-mysql](https://github.com/siddontang/go-mysql)                                    | Go                      | Pass, not familiar with go                                                                      |
| [tungsten-queue-applier](https://github.com/tailorcai/tungsten-queue-applier)         | Java                    | Pass, unmaintained                                                                              |

Binlog dump, we use the open source
program [php-mysql-replication](https://github.com/krowinski/php-mysql-replication)

### Overall structure 

![C:\\05382c6ae133c564513afc3eebcdd2e0](media/2588e5543c478d774de467100f8a8235.tmp)

### Fundamental

A cdc program (here referred to as a running instance) is connected to the mysql
binlog, which is equivalent to adding a slave library to mysql.

Read the binlog event through the main thread, send it to the queue, read the
corresponding queue event by the worker thread, and distribute the event

Here are two kinds of sending strategies:

1.  Does not guarantee the order of sending, only concerned with the database
    change operation, the default provides four distribution threads (worker
    thread), corresponding blocking queue

2.  To ensure the sequence of events sent, seq worker threads send events
    corresponding to seq blocking queue

Two kinds of strategies can be selected according to actual needs, and the
former has better performance.

Send event to convert binlog binary stream to JSON format string. This project
provides rabbitmq message listener and sends messages to rabbitmq TOPIC type
exchanges.

During the binlog event dump process, all current database changes from the
mysql are obtained. In actual use, we may only need to focus on the change
events of a particular table. This project introduces event filtering
logic. Binlog events are filtered by configuring the corresponding database +
table name.

### ETCD

[Etcd](https://coreos.com/etcd/) is a highly available, strongly consistent
service discovery repository. Cdc mainly implements the following functions
through etcd:

1.  Service registration

2.  Distributed lock

3.  Save binlog file name and postion

4.  Configuration management

5.  Configure dynamic loading (reload)

#### Service registration

After the instance is started, a unique key is registered in etcd (the design
principle of key is described in detail in the [configuration
management](#configuration-management) section). The key lifetime is 60 seconds. Refresh the ttl every 5s by turning on
the thread (helper-thread3) and keep heartbeat with etcd.

The main purpose of service registration is to monitor the status of the
cluster. Assuming that the program is interrupted abnormally or the server hangs
up, after 60s, the registered key of etcd expires, so that the faulty machine
can be accurately located and recovered quickly.

#### Distributed lock

In order to ensure high availability of the cdc service. By starting the cdc
program on multiple servers at the same time, monitor the same slave
library. Then introduce a distributed lock to ensure that only one instance is
running at the same time.

Specific to program operation

1.  Try to get distributed lock from etcd

2.  If successful, the connection to the slave is started and the binlog file
    stream event is obtained.

3.  Failed to get, then the main thread waits 5 seconds and then goes back to
    step 1

We uniquely identify a lock through the cluster + instance. The concept of
clustering was introduced to be compatible with such scenarios: 2 instances are
open at the same time to listen to the same slave (not recommended). The default
cluster is master

#### Save binlog name and location node

The helper thread (helper-thread1) synchronizes the binlog name and position of
the most recent successful message and saves it to etcd at a frequency of 5
seconds. Take the earliest binlog position synchronization

For example, the five sending thread positions are:

>   fileName=mysql-bin.000004,pos=36064

>   fileName=mysql-bin.000004,pos=37232

>   fileName=mysql-bin.000004,pos=35556

>   fileName=mysql-bin.000004,pos=36648

>   fileName=mysql-bin.000003,pos=36648

The final saved is mysql-bin.000003,36648. The next time you start the instance,
it reads the position from etcd and continues to parse the binlog event from
that position.

> Note: There will be repeated messages when the program restarts

#### Configuration management

Unified management configuration through etcd. The configuration here mainly
includes:

-   Mysql:

>   Mysql.host = 10.0.0.1   
>   Mysql.port = 3300   
>   Mysql.username =admin   
>   Mysql.password = 123

-   Rabbitmq:

>   Rabbitmq.host = 10.0.0.1   
>   Rabbitmq.port = 5672   
>   Rabbitmq.vhost = core   
>   Rabbitmq.username =admin   
>   Rabbitmq.password = 123   
>   Rabbitmq.exchangename = cdc

-   Database table configuration, reconnection timeout, retries

Since etcd is a kv structure, the key rules mapped to etcd are as follows:

Uniform prefix cdc/cluster/instance, below is a complete list of configurations

| **Key**                                                            | **Value**         | **Required** | **meaning**                                                                                                                                                       |
|--------------------------------------------------------------------|-------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cdc/master/admin/config/app/mysql/host                    | 10.0.0.1          | Yes          | Database host ip                                                                                                                                                  |
| cdc/master/admin/config/app/mysql/port                    | 3300              | Yes          | Database port                                                                                                                                                     |
| cdc/master/admin/config/app/mysql/username                | Admin             | Yes          | Database username                                                                                                                                                 |
| cdc/master/admin/config/app/mysql/password                | 123               | Yes          | Database password                                                                                                                                                 |
| cdc/master/admin/config/app/rabbitmq/host                 | 10.0.0.1,10.0.0.2 | Optional     | Rabbitmqcluster host ip (comma separated) if you use rabbitmq required                                                                                            |
| cdc/master/admin/config/app/rabbitmq/port                 | 4567              | Optional     | Rabbitmq cluster port (only support cluster port is the same when multiple clusters are configured )                                                              |
| cdc/master/admin/config/app/rabbitmq/username             | Admin             | Optional     | Rabbitmq cluster username                                                                                                                                         |
| cdc/master/admin/config/app/rabbitmq/password             | 123               | Optional     | Rabbitmq cluster password                                                                                                                                         |
| cdc/master/admin/config/app/rabbitmq/vhost                | Core              | Optional     | Rabbitmq cluster Virtual host                                                                                                                                     |
| cdc/master/admin/config/app/rabbitmq/exchangename         | Cdc               | Optional     | Rabbitmq cluster exchange name                                                                                                                                    |
| cdc/master/admin/config/cdc/connection_timedout_inseconds | 10                | no           | Cdc and mysql connection timeout (default 10s)                                                                                                                    |
| cdc/master/admin/config/cdc/ping_failed_max_retry         | 50                | no           | Cdc and mysql retry connections                                                                                                                                   |
| cdc/master/admin/config/cdc/mysqlTimeZone                 | GMT+8             | no           | We will convert the datatime to a timestamp and add the mysql time zone ( time zone abbreviation such as CST is not allowed . There will be time zone confusion ) |
| cdc/master/admin/config/cdc/seqfilter/admin/user          | 1                 | Optional     | Send the table in order, admin.user configuration, where the value is fixed to 1                                                                                  |
| cdc/master/admin/config/cdc/filter/admin/role             | 1                 | Optional     | Non-sequential sending table, admin. roleconfiguration, where value is fixed to 1                                                                                 |
| cdc/master/admin/config/cdc/filter/admin/orders_{suffix}  | 1                 | Optional     | Non-sequential delivery table, admin.orders_{suffix } tableconfiguration, where value is fixed to 1                                                               |
##### Filtering rule
The table filtering key basic rule is:
- Sequentially sent: cdc/cluster/instance/config/cdc/filter/database name/table
name
- Non-sequential sending: cdc/cluster/instance/config/cdc/seqfilter/database name/table name

> Note: Table filtering configuration requires at least one, if this feature is enabled.
##### Sharding
For example, if you do a sharding for table orders, you
can capture the changes as long as they satisfy the orders\_ prefix table. 
> Note: the "{suffix}" is a fixed value and cannot be changed.
##### Stop table filtering
```java
client.setWatchAllTable(true);
```

In addition to configuration information, other data generated by the program
will also be stored via etcd, including:

| **Key**                                           | **Value**        | **meaning**                                                                                                                                                                                                              |
|---------------------------------------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cdc/master/admin/serverid                | 65535            | [server id](https://dev.mysql.com/doc/refman/5.6/en/replication-options.html) Automatically generated, the same instance is opened in multiple clusters, this value is guaranteed not to conflict                        |
| cdc/master/admin/binlogfile/name         | mysql-bin.000002 | Binlog file name                                                                                                                                                                                                         |
| cdc/master/admin/binlogfile/pos          | 114269237        | Binlog file postion                                                                                                                                                                                                      |
| cdc/master/admin/register/administor/123 | 1 or 2           | 1 indicates that the registration is successful, 2 indicates that the lock is successfully acquired, and the status is conveniently monitored by the set value. Hostname + pid to uniquely identify the registration key |
| cdc/master/admin/lock/6f4e62f6aa254dbc   | no               | Lock key                                                                                                                                                                                                                 |

#### Configure dynamic loading

Currently supports dynamically adding tables and deleting table configurations,
such as adding tables to etcd, adding the corresponding key
value:cdc/master/admin/config/cdc/filter/admin/right 1.

The helper-thread2 watch this key to achieve dynamic loading configuration.

> Note: databases and other configuration information are not support to
dynamically loaded.

### Message receiver

The mysql binlog event is parsed into a JSON string. You can select the message
receiver. By default, the rabbitmq message receiver is implemented. If you need
to use other message receivers, you can implement the message sending method by
inheriting CdcClient.MessageListener. When you start, you add the message
receiver. Can support multiple message receivers.

#### Rabbitmq message sent

The CDC will send the message to a TOPIC-type exchange in rabbitmq. Visible
documentation:[Rabbitmq TOPIC
Introduction](https://translate.google.com/translate?hl=zh-CN&prev=_t&sl=zh-CN&tl=en&u=http://www.rabbitmq.com/tutorials/tutorial-five-python.html)

According to the corresponding routing key of the message, it is very convenient
to send the message to the specified queue. Currently CDC's routingkey
generation rules are: Database(database name).table(table
name).action(corresponding action, insert/update/delete)

If it is a sharding table, in order to avoid adding multiple bindings in
rabbitmq. The routingkey generation rule is: database(database
name).{table(table name prefix)}.action(corresponding action,
insert/update/delete). For example, if the orders are orders_1 and orders_2, the
corresponding routingkey is admin.{orders_}.\*

The steps to set up an enqueuing rule are basically as follows:

1.  Create a new queue, such as all-users-binlog

2.  Create a binding relationship in this exchange of "cdc", as shown in the
    example:

 

![C:\\3eac33f77ea9f1fcbcae3113779d93d1](media/f63ae49bd3cac52edaf4449b9f52c7d7.tmp)

Register rabbitmq message listener
```java
client.registerMessageListener(new RabbitMessageListener());
```

### Message

In order to be compatible with different message receivers, the binlog event is
uniformly converted to a JSON string. For the binlog_row_image format is full or
minimal, but the update data contains all the fields, the basic structure is
consistent

Time-related will be converted into a timestamp
> Note: that the time field is relative to 00:00:00, 01/01/1970 GMT

#### INSERT

| **key**    | **meaning**                                |
|------------|--------------------------------------------|
| database   | database name                              |
| createtime | Time for generating this message           |
| data       | Details of fields inserted in this message |
| action     | The operation type of this data            |
| uniqid     | message of md5(except createtime)          |
| table      | table name                                 |
```json
{
    "database":"test",
    "createtime":1525319434400,
    "data":{
        "longtext":"test longtext",
        "date":"1556121600000",
        "year":"2019",
        "bit":"1",
        "point":"POINT (123.46202 41.2301)",
        "smallint":"23",
        "datetime":"1556007145000",
        "text":"test text",
        "fload":"5.55",
        "bigint":"55",
        "tinyblob":"test tinyblob",
        "timestamp":"1524801357000",
        "multipoint":"POINT (123.46222 41.201)",
        "mediumint":"5",
        "set":"a,c,d",
        "mediumtext":"test mediumtext",
        "double":"5.555",
        "tinytext":"test tinytext",
        "varchar":"test varchar",
        "tinyint":"7",
        "multilinestring":"LINESTRING (0 0, 13 16)",
        "int":"6",
        "enum":"a",
        "mediumblob":"test mediumblob",
        "varbinary":"convert(binary,'8280')",
        "multipolygon":"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 7 5, 7 7, 5 7, 5 5))",
        "blob":"test ccblob",
        "polygon":"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 7 4, 7 7, 4 7, 4 4))",
        "binary":"convert(binary,'828')",
        "char":"test char",
        "longblob":"test longblob",
        "geometry":"POINT (108.998710632 34.258825935)",
        "time":"-10800000",
        "geometrycollection":"POINT (10 30)",
        "decimal":"5.56",
        "linestring":"LINESTRING (0 0, 10 10, 20 25)"
    },
    "action":"insert",
    "uniqid":"fe5be0ecaee15e7331a9c841df5c0b91",
    "table":"cdctest"
}
```
#### UPDATE

| **key**    | **meaning**                                                                                                                    |
|------------|--------------------------------------------------------------------------------------------------------------------------------|
| database   | database name                                                                                                                  |
| createtime | Time for generating this message                                                                                               |
| data       | before:The fields and values before the update, are generally primary keys or UK after:Updated fields and corresponding values |
| action     | The operation type of this data                                                                                                |
| uniqid     | message of md5(except createtime)                                                                                              |
| table      | table name                                                                                                                     |

```json
{
    "database":"test",
    "createtime":1525319725963,
    "data":{
        "before":{
            "int":"6"
        },
        "after":{
            "mediumint":"3",
            "decimal":"5.50",
            "bigint":"2",
            "timestamp":"1525319725000"
        }
    },
    "action":"update",
    "uniqid":"8e9c8af3acc6a7e6ff0d6c23608b4eaf",
    "table":"cdctest"
}
```
#### DELETE

| **key**    | **meaning**                                                                 |
|------------|-----------------------------------------------------------------------------|
| database   | database name                                                               |
| createtime | Time for generating this message                                            |
| data       | This deleted field and corresponding value are generally primary keys or UK |
| action     | The operation type of this data                                             |
| uniqid     | message of md5(except createtime)                                           |
| table      | table name                                                                  |

```JSON
{
    "database":"test",
    "createtime":1525319817439,
    "data":{
        "int":"6"
    },
    "action":"delete",
    "uniqid":"9f0460b56a10fe87713093d13c2b7226",
    "table":"cdctest"
}
```

The table structure in this example is as follows
```sql
CREATE TABLE `cdctest` (
  `int` int(11) NOT NULL AUTO_INCREMENT,
  `tinyint` tinyint(4) DEFAULT NULL,
  `smallint` smallint(6) DEFAULT NULL,
  `mediumint` mediumint(9) DEFAULT NULL,
  `bigint` bigint(20) DEFAULT NULL,
  `fload` float DEFAULT NULL,
  `double` double DEFAULT NULL,
  `decimal` decimal(10,2) DEFAULT NULL,
  `enum` enum('a','b','c','d') DEFAULT NULL,
  `bit` bit(1) DEFAULT NULL,
  `char` char(255) DEFAULT NULL,
  `varchar` varchar(255) DEFAULT NULL,
  `tinytext` tinytext,
  `mediumtext` mediumtext,
  `text` text,
  `longtext` longtext,
  `blob` blob,
  `tinyblob` tinyblob,
  `mediumblob` mediumblob,
  `longblob` longblob,
  `set` set('a','b','c','d') DEFAULT NULL,
  `year` year(4) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `time` time DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `timestamp` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `linestring` linestring DEFAULT NULL,
  `multilinestring` multilinestring DEFAULT NULL,
  `binary` binary(255) DEFAULT NULL,
  `varbinary` varbinary(255) DEFAULT NULL,
  `point` point DEFAULT NULL,
  `multipoint` multipoint DEFAULT NULL,
  `multipolygon` multipolygon DEFAULT NULL,
  `polygon` polygon DEFAULT NULL,
  `geometry` geometry DEFAULT NULL,
  `geometrycollection` geometrycollection DEFAULT NULL,
  PRIMARY KEY (`int`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
```
