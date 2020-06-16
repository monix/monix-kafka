---
id: producer
title: Producer
---

 ```scala
 import monix.kafka._
 import monix.execution.Scheduler
 
 implicit val scheduler: Scheduler = monix.execution.Scheduler.global
 
 // Init
 val producerCfg = KafkaProducerConfig.default.copy(
   bootstrapServers = List("127.0.0.1:9092")
 )
 
 val producer = KafkaProducer[String,String](producerCfg, scheduler)
 
 // For sending one message
 val recordMetadataF = producer.send("my-topic", "my-message").runToFuture
 
 // For closing the producer connection
 val closeF = producer.close().runToFuture
 ```
 
 Calling `producer.send` returns a [Task](https://monix.io/docs/3x/eval/task.html) of `Option[RecordMetadata]` which can then be run and transformed into a `Future`.
 
 If the `Task` completes with `None` it means that `producer.send` method was called after the producer was closed and that the message wasn't successfully acknowledged by the Kafka broker. In case of the failure of the underlying Kafka client the producer will bubble up the exception and fail the `Task`.  All successfully delivered messages will complete with `Some[RecordMetadata]`.
 
 For pushing an entire `Observable` to Apache Kafka:
 
 ```scala
 import monix.kafka._
 import monix.execution.Scheduler
 import monix.reactive.Observable
 import org.apache.kafka.clients.producer.ProducerRecord
 
 implicit val scheduler: Scheduler = monix.execution.Scheduler.global
 
 // Initializing the producer
 val producerCfg = KafkaProducerConfig.default.copy(
   bootstrapServers = List("127.0.0.1:9092")
 )
 
 val producer = KafkaProducerSink[String,String](producerCfg, scheduler)
 
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
   .runToFuture
 ```


