---
id: consumer
title: Consumer
---

### Introduction

Apache Kafka does provide a wide range of parameters to be configured, it allows to cover the most specific business cases and also highly recommendable to fine tune them for reaching out the best 
possible performance.

Monix Kafka designed to provide file driven configuration to the user's application, so the values set on [default.conf](/examples/src/it/resources/default.conf) represents the default configuration used 
by either the Consumer or the Producer. But it will be necessary to override these config parameters either from the same conf file, from environment variables or directly from the code. 

Let's see how to do so in the following section.

### Getting started with configuration

As mentioned before, you can specify configuration parameters from `.conf`. This has to be `HOCON` file, 
an awesome file format that supports many features to fit your use case, it does support java format, substitutions, comments, properties-like notation, and more importantly, allow to substitute from environment variables. 
For more info see: [typesafe config](https://github.com/lightbend/config).

As a quick go through the configuration, let's highlight some of the most important configuration fields:

The first one and more important one is the kafka broker you are pointing to, being as default `localhost:9092` would probably don't need to be modified to work locally, but you would definetly 
have to update this one with the required bootstrap servers of your kafka cluster:

```hocon
kafka {
  bootstrap.servers = ["localhost:9092", "localhost:9093"]
  client.id = ""
  ...
}
```

You could for example overwrite the client id from an env var like:

```hocon
  client.id = ""
  client.id = ${KAFKA_CLIENT_ID}
```

Or if you put an interrogant before the env var name, it would just use it in case the variable exists, otherwise would just fallback to the default value:

```hocon
  client.id = "default-client-id"
  client.id = ${?KAFKA_CLIENT_ID}
```

Finally, you can just set all these values from your code in a very nice way:

```scala
import monix.kafka._

val consumerConf = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
)
```

There are roughly 70 fields to be configured, mostly they are related with security, broker, topic, communication, producer and consumer.

For more information about those, you would better consult either the kafka or confluent [configuration documentation](https://docs.confluent.io/current/installation/configuration/index.html).  

