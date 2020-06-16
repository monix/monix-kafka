---
id: consumer
title: Consumer
---

Apache Kafka does provide a wide range of parameters to be configured, it allows to cover the most specific business cases and also highly recommendable to fine tune them for reaching out the best 
possible performance.


Monix Kafka designed to provide file driven configuration to the user's application, so the values set on [default.conf](/ka) would represents the default configuration 





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



