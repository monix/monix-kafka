package monix.kafka.benchmarks

import monix.kafka.config.ObservableCommitType

import java.util.concurrent.TimeUnit
import monix.kafka.{KafkaConsumerObservable, KafkaProducerSink}
//import monix.kafka.config.ObservableCommitType
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup, _}
import scala.concurrent.duration._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 1)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class ConsumerBenchmark extends MonixFixture {

  var totalRecords: Int = 1500
  val consumedRecords = 1000
  var maxPollRecords: Int = 1

  // preparing test data
  @Setup
  def setup(): Unit = {
    Observable
      .from(0 to totalRecords)
      .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
      .bufferTumbling(totalRecords)
      .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_auto_commit10ms(): Unit = {
    val conf = consumerConf.value().copy(
      maxPollRecords = maxPollRecords,
      observableCommitType = ObservableCommitType.Async)
      .withPollHeartBeatRate(10.millis)

    KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }


  @Benchmark
  def monix_manual_commit_heartbeat1ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat10ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(10.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat100ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(100.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat1000ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1000.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }


}
