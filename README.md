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