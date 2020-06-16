---
id: consumer
title: Consumer
---


In order to understand how to read data from Kafka, you first need to understand the concept of a Kafka consumer and consumer groups.

Monix Kafka implements the Consumer API in form of monix `Observable` type, which would represent an unbounded stream of events consumed from the specified kafka topics.


There are several ways for consuming from Apache Kafka (Version 0.11.x and above):



### Auto commit consumer


```scala
import monix.kafka._

val consumerConf = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
  // you can use this settings for At Most Once semantics:
  // observableCommitOrder = ObservableCommitOrder.BeforeAck
)

val observable =
  KafkaConsumerObservable[String,String](consumerConf, List("my-topic"))
    .take(10000)
    .map(_.value())
```

Consumer which allows you to commit offsets manually:
```scala
import monix.kafka._

val consumerCfg = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
)

val observable =
  KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List("my-topic"))
    .map(message => message.record.value() -> message.committableOffset)
    .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    .bufferTimedAndCounted(1.second, 1000)
    .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
```



