---
id: configuration
title: Configuration
---

### Introduction

Apache Kafka does provide a wide range of parameters to be configured, it allows to cover the most specific business cases and also highly recommendable to fine tune them for reaching out the best 
possible performance.

Monix Kafka provides file driven configuration to the user's application, so it means that the values set on [default.conf](https://github.com/monix/monix-kafka/blob/master/kafka-1.0.x/src/main/resources/monix/kafka/default.conf) represents the kafka configuration used by default. 

If a file with format `.conf` is in the folder `src/resource` of your project, will be used as a default one and from there on you can overwrite using environment variables or directly from the code. 

Let's see how to do so in the following section.

### Getting started with the configuration

As mentioned before, you can specify configuration parameters from `.conf`. It represents a `HOCON` file, 
an awesome file format that supports many features to fit your use case, with support for java format, substitutions, comments, properties-like notation, and more importantly, it allows substitution from environment variables and from your code. 
For more info see: [typesafe config](https://github.com/lightbend/config).

As a quick go through, let's highlight some of the most important configuration fields:

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

There are roughly 70 fields to be configured, mostly they are related with security, broker, topic, communication, consumer and producer.

For more information about those, you would better consult either the kafka or confluent [configuration documentation](https://docs.confluent.io/current/installation/configuration/index.html).  

Consumer and Producer specific configurations would be explained in detail on their respective sections.
