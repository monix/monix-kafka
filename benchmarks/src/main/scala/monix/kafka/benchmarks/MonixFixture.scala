package monix.kafka.benchmarks

import monix.eval.Coeval
import monix.execution.Scheduler
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaProducerConfig}
//import monix.reactive.Observable
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.clients.producer.ProducerRecord

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
    autoOffsetReset = AutoOffsetReset.Earliest
  ))

}
