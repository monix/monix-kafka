---
id: producer
title: Producer
---

The _Monix Kafka_ producer module relies in the underlying _Kafka Producer API_ that would allow the application to asynchronously publish on one or more Kafka topics. 

Below table describes the two available ways of publishing events to Kafka, which both complements very well to accomplish different possible use cases, by either producing a single event or instead using the producer sink that will push an unbounded stream of events.


  | __Signature__ | __Expects__  | __Input__ | __Described by__ |
  | :---: | :---: | :---: | :---: |
  | _KafkaProducer.send_ | Single record | `ProducerRecord[K, V]`, (`K`, `V`) or just `V`  | `Task` |
  | _KafkaProdcuerSink.apply_ | Multiple records | `Observable[Seq[ProducerRecord[K, V]]]` | `Consumer[Seq[ProducerRecord[K, V]], Unit]` |

More details and examples can be found in the next sections:

## Producer Configurations 

This section only mentions the producer related configurable parameters, but you might need to set other kafka configurations that can affect producer behaviour too such like buffer memory and size, security protocols and others.
As you might notice, there are not as much configurable parameters for the _Kafka Producer_ than there is for _Consumer_, but still are quite important in regards to get the best possible performance.

```hocon
kafka {
  # N. of times for the client to resend any record whose send fails with a potentially transient error.
  retries = 0
  # N. of requests that KafkaProducerSink can push in parallel
  monix.producer.sink.parallelism = 100
}
```

For more details about all the configurable parameters, please directly review the [official confluent documentation](https://docs.confluent.io/current/installation/configuration/producer-configs.html) 
for _Kafka Producer Configuration_.
You could also refer to `monix.kafka.KafkaProducerConfig` in order to know exactly what are the properties that the producer expects.

## Single record producer

 The best way of asynchronously producing a single record to _Kafka_ is by using the `.send` method from `monix.kafka.KafkaProducer`. 
 It accepts different inputs, being a `ProducerRecord[K, V]`, (`K`, `V`) or just the `V`, and returns a [Task](https://monix.io/docs/3x/eval/task.html) of `Option[RecordMetadata]` can later be run and transformed into a `Future`, that:
 
 - If it completes with `None` it means that `producer.send` method was called after the producer was closed and therefore the message wasn't successfully acknowledged by the Kafka broker.
 
 - In case of failure reported by the underlying _Kafka client_, the producer will bubble up the exception and fail the `Task`. 
  
 - Finally, all successfully delivered messages will complete with `Some[RecordMetadata]`.
 
 ```scala
 import monix.kafka._
  
 implicit val scheduler: Scheduler = monix.execution.Scheduler.global
 // init producer configuration
 val producerConf = KafkaProducerConfig.default.copy(
   bootstrapServers = List("127.0.0.1:9092")
 )
 
 // builds monix kafka producer
 val producer = KafkaProducer[String,String](producerConf, scheduler)
 
 // sends a single message
 val recordMetadataF: Future[Option[RecordMetadata]] = producer.send("my-topic", "my-message").runToFuture
 
 // closes the connection
 val closeF = producer.close().runToFuture
 ```
 
 ## Sink producer 
 
 On the other hand, if an unbounded number of records needs to be produced, it is better to use `monix.kafka.KafkaProducerSink`, which provides the logic for pushing an `Observable[ProducerRecord[K, V]]` to the specified _Kafka_ topics.
 
  As it was mentioned in the producer configuration section, `monix.producer.sink.parallelism` allows to specify the parallelism on producing requests from `KafkaProducerSink`. 
 
 See below an example on how to use the kafka producer sink:
 
 ```scala
 import monix.kafka._
 import monix.reactive.Observable
 import org.apache.kafka.clients.producer.ProducerRecord
 
 implicit val scheduler: Scheduler = monix.execution.Scheduler.global
 
 // init producer configuration
 val producerConf = KafkaProducerConfig.default.copy(
   bootstrapServers = List("127.0.0.1:9092"),
   monixSinkParallelism = 3
 )
 
 val producer = KafkaProducerSink[String,String](producerConf, scheduler)
 
 // lets pretend we have this observable of records
 val observable: Observable[ProducerRecord[String,String]] = ???
 
 observable
   // on overflow, start dropping incoming events
   .whileBusyDrop
   // buffers into batches if the consumer is busy, up to a max size
   .bufferIntrospective(1024)
   // consume everything by pushing into Apache Kafka
   .consumeWith(producer)
   // ready, set, go!
   .runToFuture
 ```


