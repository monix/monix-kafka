# Monix-Kafka

Monix integration with Kafka

Work in progress!

## Getting Started with Kafka 0.8.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-8" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.8.x` from their 
[download page](https://kafka.apache.org/downloads.html) (note that
`0.9.x` or higher do not work with `0.8`), then as the
[quick start](https://kafka.apache.org/082/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka8/test
```

## Getting Started with Kafka 0.9.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-9" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.9.x` from their 
[download page](https://kafka.apache.org/downloads.html) (note that
`0.10.x` or higher do not work with `0.9`), then as the
[quick start](https://kafka.apache.org/090/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka9/test
```

## Getting Started with Kafka 0.10.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-10" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.10.x` from their 
[download page](https://kafka.apache.org/downloads.html), then as the
[quick start](https://kafka.apache.org/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka10/test
```


## Getting Started with Kafka 0.11.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-11" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.11.x` from their 
[download page](https://kafka.apache.org/downloads.html), then as the
[quick start](https://kafka.apache.org/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka11/test
```

## Getting Started with Kafka 1.0.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-1x" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `1.0.x` from their 
[download page](https://kafka.apache.org/downloads.html), then as the
[quick start](https://kafka.apache.org/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka1x/test
```

## Usage

The producer:

```scala
import monix.kafka._

// Init
val producerCfg = KafkaProducerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092")
)

val producer = KafkaProducer[String,String](producerCfg, io)

// For sending one message
val recordMetadataF = producer.send("my-topic", "my-message").runAsync

// For closing the producer connection
val closeF = producer.close().runAsync
```

Note that these methods return [Tasks](https://monix.io/docs/2x/eval/task.html),
which can then be transformed into `Future`.

For pushing an entire `Observable` to Apache Kafka:

```scala
import monix.kafka._
import org.apache.kafka.clients.producer.ProducerRecord

// Initializing the producer
val producerCfg = KafkaProducerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092")
)

val producer = KafkaProducerSink[String,String](producerCfg, io)

// Lets pretend we have this observable of records
val observable: Observable[ProducerRecord[String,String]] = ???

observable
  // on overflow, start dropping incoming events
  .whileBusyDrop
  // buffers into batches if the consumer is busy, up to a max size
  .bufferIntrospective(1024)
  // consume everything by pushing into Apache Kafka
  .consumeWith(producer)
  // ready, set, go!
  .runAsync
```

For consuming from Apache Kafka (Version 0.11.x and above):

```scala
import monix.kafka._

val consumerCfg = KafkaConsumerConfig.default.copy( 
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
)

val observable = 
  KafkaConsumerObservable[String,String](consumerCfg, List("my-topic"))
```

Enjoy! 

## How can I contribute to Monix-Kafka?

We welcome contributions to all projects in the Monix organization and would love 
for you to help build Monix-Kafka. See our [contributor guide](./CONTRIBUTING.md) for
more information about how you can get involed.

## Maintainers

The current maintainers (people who can merge pull requests) are:

- Alexandru Nedelcu ([alexandru](https://github.com/alexandru))
- Alex Gryzlov ([clayrat](https://github.com/clayrat))

## License

All code in this repository is licensed under the Apache License,
Version 2.0.  See [LICENCE.txt](./LICENSE.txt).
