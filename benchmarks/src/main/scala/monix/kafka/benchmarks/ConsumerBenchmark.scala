package monix.kafka.benchmarks

import java.util.concurrent.TimeUnit
//import monix.execution.Scheduler
import monix.kafka.{KafkaConsumerObservable, KafkaProducerSink}
import monix.kafka.config.ObservableCommitType
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup, _}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 1)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class ConsumerBenchmark extends MonixFixture {

  var size: Int = 1000
  var maxPollRecords: Int = 5

  // preparing test data
  val f = Observable
    .from(1 to size * 10)
    .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
    .bufferTumbling(size)
    .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
    .runToFuture(io)
  Await.ready(f, 10.seconds)

  @Benchmark
  def monix_manual_commit(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(size)
      .headL
    Await.result(f, Duration.Inf)
  }

  @Benchmark
  def monix_auto_commit(): Unit = {
    val conf = consumerConf.value().copy(
      maxPollRecords = maxPollRecords,
      observablePollHeartbeatRate = 1.milli,
      observableCommitType = ObservableCommitType.Async)
    KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
      .take(size)
      .headL
    Await.result(f, Duration.Inf)
  }

}
