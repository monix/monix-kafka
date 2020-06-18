---
id: consumer
title: Consumer
---

_Monix Kafka_ abstracts the _Kafka Consumer API_ in form of `Observable` type, which would represent an unbounded stream of events consumed from the specified kafka topics.

Below table shows the two different ways of consuming from Kafka topics are available (Version 0.11.x and above):

| __Offsets handling__ | __Signature__  | __Stream element type__ |
  | :---: | :---: | :---: |
  | No _(auto commit can be enabled)_| _KafkaConsumerObservable.apply_ | _ConsumerRecord[K, V]_ |
  | Manual commit | _KafkaConsumerObservable.manualCommit_ | _CommittableMessage[K, V]_ |
  
These two will be further explained in code on next sections, but first let's review the _Consumer configuration_.
  
### Consumer configuration

As it was mentioned on the [previous]() sections, configuration can be specified either from [default.conf](https://github.com/monix/monix-kafka/blob/master/kafka-1.0.x/src/main/resources/monix/kafka/default.conf#L49) or 
overwriting default values from the same code. Below list of properties represents the parameters
of the _HOCON_ file that are __only__ consumer related properties which could give you a first glance of what to look at when configuring the consumer, but of course there 
are more configurable parameters related to Kafka:

```hocon
kafka {
  # these represents ony consumer related configurable parameters
  # but you might want to set other kafka configurations that affect can consumer too
  fetch.min.bytes = 1
  fetch.max.bytes = 52428800
  group.id = "" 
  heartbeat.interval.ms = 3000
  max.partition.fetch.bytes = 1048576
  session.timeout.ms = 10000
  auto.offset.reset = "latest"
  enable.auto.commit = false
  exclude.internal.topics = true
  receive.buffer.bytes = 65536
  check.crcs = true
  fetch.max.wait.ms = 500
  session.timeout.ms = 10000
  max.poll.records = 500
  max.poll.interval.ms = 300000
  # sync, async
  monix.observable.commit.type = "sync"
  # before-ack, after-ack or no-ack
  monix.observable.commit.order = "after-ack"
}
```

For more details about what each of these configurable parameters mean, please directly review the [official confluent documentation](https://docs.confluent.io/current/installation/configuration/consumer-configs.html#cp-config-consumer) 
for _Kafka Consumer Configuration_.
You would also better check `monix.kafka.KafkaConsumerConfig` in order to know exactly what are the properties the consumer takes care of.
Note that `monix.observable.commit.type` and `monix.observable.commit.order` are not passed to Kafka, since they are monix self configurations that would be taken into account only for the 
`plain consumer` but not for `manualCommit`. See next section for more info about these.  

### Plain consumer

The `plainSource` emits `ConsumerRecord` elements, a record represents the received key/value pair that also contains information about the topic, partition, offset and timestamp). 
But more importantly, it __does not support offsets commitment__ to Kafka, then it can be used when the offset is stored externally or with auto-commit.

Note that auto-commit is disabled by default, you can fine tune the auto commitment by setting the monix observable specific configuration `ObservableCommitOrder`,  
which will allow you to decide whether to commit the records before receiving an acknowledgement from downstream, after that, or to just don't acknowledge (as a default one). 

If _At Most Once_ semantics is seek, auto commit must be enabled and observable commit order done before ack:
 
```scala
import monix.kafka._

val consumerConf = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests",
  enableAutoCommit = true,
  observableCommitOrder = ObservableCommitOrder.BeforeAck
)
```

If the concept of _auto-commit_ and _observableCommitOrder_ was well understood, the implementation will be straight forward for you:

```scala
import monix.kafka._

val observable =
  KafkaConsumerObservable[String,String](consumerConf, List("my-topic"))
    .take(10000)
    .map(_.value())
```

### Manual commit consumer:

The `manualCommit` makes it possible to commit offset positions to Kafka. In this case the emitted record would be `CommitableMessage`, 
this is just a wrapper for `ConsumerRecord` with `CommittableOffset`.

Committable offset represents the offset for specified topic and partition that can be committed synchronously by `commitSync` method call or asynchronously by one of commitAsync methods.
 To achieve good performance it is recommended to use batched commit with `CommittableOffsetBatch` class.
  
Let's now see an example on how to use the batch committable offset:

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

In summary, this consumer is useful when _At Least Once_ delivery is desired, as each message will be delivered at least once but in failure cases could be duplicated.

And compared with _auto commit_, it gives fine granted control over when a message is considered consumed or not.

