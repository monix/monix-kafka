package monix.kafka.benchmarks

import cats.effect.IO
import monix.eval.Coeval
import monix.execution.Scheduler
import monix.kafka.config.{AutoOffsetReset => MonixAutoOffsetReset}
import monix.kafka.{KafkaConsumerConfig, KafkaProducerConfig}
//import monix.reactive.Observable
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.clients.producer.ProducerRecord
import fs2.kafka._

trait MonixFixture extends BaseFixture{

  implicit lazy val io = Scheduler.io("monix-kafka-benchmark")

  val producerConf = KafkaProducerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = monixTopic,
  )

  // we set a different group id every time so then the consumer will always
  // read from the beginning
  val consumerConf = Coeval(KafkaConsumerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = monixTopic,
    groupId = randomId.value(),
    enableAutoCommit = false,
    autoOffsetReset = MonixAutoOffsetReset.Earliest
  ))

  val fs2ConsumerSettings: ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group")

  val fs2ProducerSettings: ProducerSettings[IO, String, String] =
    ProducerSettings[IO, String, String]
      .withBootstrapServers("localhost:9092")



}
