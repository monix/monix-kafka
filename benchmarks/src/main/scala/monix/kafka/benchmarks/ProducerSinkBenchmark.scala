package monix.kafka.benchmarks

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup, _}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class ProducerSinkBenchmark extends MonixFixture {

  var size: Int = 100

  @Benchmark
  def parallel_1P_1RF(): Unit = {
    val f = produceGroupedSink(topic_sink_1P_1RF, size, 1, 100).runToFuture(io)
    Await.ready(f, Duration.Inf)
  }


  @Benchmark
  def parallel_2P_1RF(): Unit = {
    val f = produceGroupedSink(topic_sink_2P_1RF, size, 1, 100).runToFuture(io)
    Await.ready(f, Duration.Inf)
  }


  @Benchmark
  def sync_sync_1P_1RF(): Unit = {
    val f = produceGroupedSink(topic_sink_1P_1RF, size, 1, 1).runToFuture(singleThread)
    Await.ready(f, Duration.Inf)
  }


  @Benchmark
  def sync_2P_1RF(): Unit = {
    val f = produceGroupedSink(topic_sink_2P_1RF, size, 1, 1).runToFuture(singleThread)
    Await.ready(f, Duration.Inf)
  }




}
