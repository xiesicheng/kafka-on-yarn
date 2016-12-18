Kafka On Yarn

---

该项目用于把 Kafka 运行在 Yarn 的辅助项目.

该项目依赖: kafka, kafka-offset-console. 见 kafka-yarn.properties 的配置

### kafka lib(kafka.zip):

    ./avro-1.7.7.jar
    ./avro-ipc-1.7.7.jar
    ./commons-cli-1.2.jar
    ./commons-collections-3.2.1.jar
    ./commons-configuration-1.6.jar
    ./commons-io-2.4.jar
    ./commons-lang-2.6.jar
    ./commons-logging-1.1.1.jar
    ./guava-14.0.1.jar
    ./hadoop-annotations-2.6.5.jar
    ./hadoop-auth-2.6.5.jar
    ./hadoop-common-2.6.5.jar
    ./hadoop-hdfs-2.6.5.jar
    ./hadoop-mapreduce-client-core-2.6.5.jar
    ./hadoop-yarn-api-2.6.5.jar
    ./hadoop-yarn-client-2.6.5.jar
    ./hadoop-yarn-common-2.6.5.jar
    ./htrace-core-3.0.4.jar
    ./jackson-core-asl-1.9.13.jar
    ./jackson-mapper-asl-1.9.13.jar
    ./joda-time-2.3.jar
    ./jopt-simple-3.2.jar
    ./kafka-yarn-0.1.0.jar
    ./kafka_2.10-0.8.1.1.jar
    ./log4j-1.2.17.jar
    ./metrics-core-2.2.0.jar
    ./netty-3.6.6.Final.jar
    ./protobuf-java-2.5.0.jar
    ./scala-library-2.10.1.jar
    ./slf4j-api-1.7.5.jar
    ./slf4j-log4j12-1.7.5.jar
    ./snappy-java-1.0.5.jar
    ./zkclient-0.3.jar
    ./zookeeper-3.3.4.jar

### kafka on yarn lib(kafka-yarn.zip):

    ./avro-1.7.7.jar
    ./avro-ipc-1.7.7.jar
    ./commons-cli-1.2.jar
    ./commons-collections-3.2.1.jar
    ./commons-configuration-1.6.jar
    ./commons-io-2.4.jar
    ./commons-lang-2.6.jar
    ./commons-logging-1.1.1.jar
    ./guava-14.0.1.jar
    ./hadoop-annotations-2.6.5.jar
    ./hadoop-auth-2.6.5.jar
    ./hadoop-common-2.6.5.jar
    ./hadoop-hdfs-2.6.5.jar
    ./hadoop-mapreduce-client-core-2.6.5.jar
    ./hadoop-yarn-api-2.6.5.jar
    ./hadoop-yarn-client-2.6.5.jar
    ./hadoop-yarn-common-2.6.5.jar
    ./htrace-core-3.0.4.jar
    ./jackson-core-asl-1.9.13.jar
    ./jackson-mapper-asl-1.9.13.jar
    ./joda-time-2.3.jar
    ./kafka-yarn-0.1.0.jar
    ./KafkaOffsetMonitor-assembly-0.2.0.jar
    ./log4j-1.2.17.jar
    ./metrics-core-2.2.0.jar
    ./netty-3.6.6.Final.jar
    ./protobuf-java-2.5.0.jar
    ./scala-library-2.10.1.jar
    ./slf4j-api-1.7.5.jar
    ./slf4j-log4j12-1.7.5.jar
    ./snappy-java-1.0.5.jar
    
上述两个包,打包后放置到HDFS /lib/kafka 下. 

## 部署指导

该项目基于Apache Hadoop 2.6.5 开发，已经在 hadoop2.6上充分测试，运行稳定。但在其他版本 Hadoop 没有测试，该项目代码架构基于我编写的其它 On Yarn 的架构，最早在hadoop 2.2.0上测试过，运行稳定。因此我认为基本可以在 Hadoop 2.2.0 之后的版本上运行。

众所周知，Hadoop 2.0 Yarn 程序都由 ApplicationMaster 管理。Yarn 上基本可以运行所有的 Master-Slave 架构。而 Kafka 是大数据工具的通用型技术，使之运行在 Yarn 上很有必要。

Kafka 是没有 Master，Slave 概念的，而 Kafka 也没有集成 Web 管理界面。github 上 Yahoo 开源的 KafkaOffsetMonitor-assembly 可以当做 Kafka 的管理界面，在 Yarn 上当做 ApplicationMaster 运行。

因此会出现如上的两个打包。

1. kafka-yarn.zip 是包含了 KafkaOffsetMonitor-assembly，yarn lib，avro rpc lib 的包集合；
2. kafka.zip 是 Kafka Broker 节点的包集合；

如果你使用的是其它的 Hadoop 版本，把内部的 Hadoop lib 换为你所使用的版本。并使用标准的 zip 打包，可以到 [下载](https://pan.baidu.com/s/1qYuB4iK)。

- 把如上两个打包后上传到 HDFS 的 /lib/kafka/ 下；
- 把该项目打包后，把 manual/kaffa-on-yarn copy 到你 kafka home 的 bin 下；
- 把 conf 下的 server.properties copy 到你 kafka 的 conf 下；
- 把 kafka-yarn.properties copy 到你 kafka 的 conf 下；
- 修改 kafka-yarn.properties yarn.kafka.appmaster.args.zk 为你的 ZooKeeper 地址；
- 把Hadoop 的配置文件 core-site.xml, hdfs-site.xml yarn-site.xml copy 到 kafka conf 下；
 
然后启动 bin/kafka-on-yarn yarn -conf conf/server.properties; 

可以通过 bin/kafka-on-yarn yarn-add -appid id -brokerid 0..n 增加 Kafka Broker；

如果有任何问题，可以联系我;

Mail: [zhzhenqin](mailto:zhzhenqin@163.com)

可以到: [zhzhenqin](http://my.oschina.net/zhzhenqin) 留言。
