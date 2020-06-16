---
id: getting-started
title: Getting Started
---

This project supports different versions of Apache Kafka, 
see in below sections how to get started with each of them:

## Kafka 1.0.x or above

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-1x" % "1.0.0-RC5"
```

For `kafka` versions higher than `1.0.x` also add a dependency override:

```scala
dependencyOverrides += "org.apache.kafka" % "kafka" % "2.1.0"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka1x/test
```

## Kafka 0.11.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-11" % "1.0.0-RC5"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka11/test
```

## Kafka 0.10.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-10" % "1.0.0-RC5"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka10/test
```

## Kafka 0.9.x

Please note that `EmbeddedKafka` is not supported for Kafka `0.9.x`

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-9" % "1.0.0-RC5"
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

## Kafka 0.8.x

Please note that support for Kafka `0.8.x` is dropped and the last available version with this dependency is `0.14`.

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

Create the topics we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-manual-commit-tests
```

And run the tests:

```bash
sbt kafka8/test
```


