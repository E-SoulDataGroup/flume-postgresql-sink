# Flume PostgreSQL Sink

## 1 使用场景

* **目前只适用于Flume是kafka，后面考虑支持其他的source，支持写入数据到多表**

## 2 快速起步

### 2.1 例子

```
#postgresql 配置
postgresql.sources=s1
postgresql.channels=c1
postgresql.sinks=k1

#s1 配置
postgresql.sources.s1.type=org.apache.flume.source.kafka.KafkaSource
postgresql.sources.s1.channels=c1
postgresql.sources.s1.batchSize=1000
postgresql.sources.s1.batchDurationMillis=2000
postgresql.sources.s1.kafka.bootstrap.servers = ip:port,ip2:port2,ip3:port3
postgresql.sources.s1.kafka.topics = topicName,topicName2
postgresql.sources.s1.kafka.consumer.group.id = flume-pg-xxxx

#c1 配置
postgresql.channels.c1.type=file
postgresql.channels.c1.checkpointDir=/data/logs/flume-pg/postgresql/file-channel/checkpoint
postgresql.channels.c1.dataDirs=/data/logs/flume-pg/postgresql/file-channel/data
postgresql.channels.c1.capacity=500000
postgresql.channels.c1.transactionCapacity=10000
postgresql.channels.c1.checkpointInterval=30000

#k1 配置
postgresql.sinks.k1.channel = c1
postgresql.sinks.k1.type = com.dh.flume.postgresql.sink.PostgreSQLSink
postgresql.sinks.k1.hostname = 192.168.xxx.xxx
postgresql.sinks.k1.port = 5432
postgresql.sinks.k1.database = databaseName
postgresql.sinks.k1.user = test
postgresql.sinks.k1.password = test
postgresql.sinks.k1.batchSize = 1000
postgresql.sinks.k1.topicToTable = [{"tableName":"tableName","writeMode":"insert","topicName":"topicName"},{"tableName":"tableName2","writeMode":"insert","topicName":"topicName2"}]
```

### 2.2 参数说明

* **只作sink的参数说明，source及channel参数说明请参考**
* **type**
	* 描述：sink类型
		* com.dh.flume.postgresql.sink.PostgreSQLSink
	* 必选：是
	* 默认值：无

* **hostname**
	* 描述：ip。
	* 必选：是
	* 默认值：无
	
* **port**
	* 描述：端口。
	* 必选：是
	* 默认值：无
	
* **user**
	* 描述：用户名。
	* 必选：是
	* 默认值：无
	
* **password**
	* 描述：密码。
	* 必选：是
	* 默认值：无
	
* **batchSize**
	* 描述：批量操作size。
	* 必选：否
	* 默认值：100
	
	
* **topicToTable**
	* 描述：kafka topic与table name的映射，json array的格式。
	* 必选：是
	* 默认值：无
	
### 2.3 编译打包

```
gradle build -x test
```









