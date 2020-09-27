package monix.kafka.benchmarks

import monix.eval.{Coeval, Task}
import monix.kafka.config.{AutoOffsetReset, ObservableCommitType}
import monix.kafka.{CommittableMessage, KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

trait MonixFixture extends BaseFixture{

  val producerConf = KafkaProducerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = topic_producer_1P_1RF,
  )

  val consumerConf = Coeval(KafkaConsumerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = topic_consumer_1P_1RF,
    groupId = randomId.value(),
    enableAutoCommit = false,
    autoOffsetReset = AutoOffsetReset.Earliest
  ))


 def produceGroupedSink(topic: String, size: Int, bufferSize: Int, parallelism: Int): Task[Unit] = {
   Observable
     .from(1 to size)
     .map(i => new ProducerRecord[Integer, Integer](topic, i))
     .bufferTumbling(bufferSize)
     .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = parallelism), io))
 }


  def consumeManualCommit(topic: String, size: Int, maxPollRecords: Int): Task[CommittableMessage[Integer, Integer]] = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(topic))
      .take(size - 1)
      .headL
  }

  def consumeAutoCommit(topic: String, consumeRecords: Int, maxPollRecords: Int, commitType: ObservableCommitType): Task[ConsumerRecord[Integer, Integer]] = {
      val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
     KafkaConsumerObservable[Integer, Integer](conf, List(topic))
      .take(consumeRecords - 1)
      .headL
  }






}
