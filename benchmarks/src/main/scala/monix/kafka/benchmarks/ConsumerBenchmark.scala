package monix.kafka.benchmarks

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
  var maxPool: Int = 5

  // preparing test data
  val t1 = produceGroupedSink(topic_consumer_1P_1RF, size * 2, 10, 1)
  val t2 = produceGroupedSink(topic_consumer_2P_1RF, size * 2, 10, 1)
  val f3 = Task.gather(List(t1, t2)).runToFuture(io)
  val _ = Await.ready(f3, Duration.Inf)

  @Benchmark
  def monix_manual_commit(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
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

 //@Benchmark
 //def monix_auto_commit(): Unit = {
 //  val conf = consumerConf.value().copy(
 //    maxPollRecords = maxPollRecords,
 //    observableCommitType = ObservableCommitType.Async)
 //  KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
 //    .take(100)
 //    .headL
 //    .runSyncUnsafe()
 //}

  @Benchmark
  def auto_commit_sync_1P_1RF(): Unit = {
    val f = consumeAutoCommit(topic_consumer_1P_1RF, size, maxPool, ObservableCommitType.Sync).runToFuture(io)
    Await.result(f, Duration.Inf)
    f.cancel()
  }

  @Benchmark
  def auto_commit_async_2P_1RF(): Unit = {
    val f = consumeAutoCommit(topic_consumer_2P_1RF, size, maxPool, ObservableCommitType.Async).runToFuture(io)
    Await.result(f, Duration.Inf)
    f.cancel()
  }

  @Benchmark
  def auto_commit_sync_2P_1RF(): Unit = {
    val f = consumeAutoCommit(topic_consumer_2P_1RF, size, maxPool, ObservableCommitType.Sync).runToFuture(io)
    Await.result(f, Duration.Inf)
    f.cancel()
  }

}
