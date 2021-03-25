package monix.kafka.benchmarks

import monix.kafka.config.ObservableCommitType

import java.util.concurrent.TimeUnit
import monix.kafka.{KafkaConsumerObservable, KafkaProducerSink}
//import monix.kafka.config.ObservableCommitType
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup, _}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 1)
@Fork(1)
@Threads(4)
class ConsumerBenchmark extends MonixFixture {

  var size: Int = 1000
  var maxPollRecords: Int = 5

  // preparing test data
  Observable
    .from(1 to size * 10)
    .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
    .bufferTumbling(size)
    .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
    .runSyncUnsafe()

  @Benchmark
  def monix_manual_commit(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(100)
      .headL
      .runSyncUnsafe()
  }


  @Benchmark
  def monix_auto_commit(): Unit = {
    val conf = consumerConf.value().copy(
      maxPollRecords = maxPollRecords,
      observableCommitType = ObservableCommitType.Async)
    KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
      .take(100)
      .headL
      .runSyncUnsafe()
  }

  /*
  @Benchmark
  def monix_manual_commit_heartbeat1(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(100)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat100(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(100.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(100)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat1000(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1000.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(100)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monix_manual_commit_heartbeat3000(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(3000.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(100)
      .headL
      .runSyncUnsafe()
  }
  */


}
