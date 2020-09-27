package monix.kafka.benchmarks

import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup}
import java.util.concurrent.TimeUnit

import monix.eval.Task
import monix.kafka.KafkaProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.openjdk.jmh.annotations._

import scala.concurrent.duration.Duration
import scala.concurrent.Await

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 1)
@Fork(1)
@Threads(1)
class ProducerBenchmark extends MonixFixture {

  var size: Int = 1000

 val producer = KafkaProducer[Integer, Integer](producerConf, singleThread)

  def produceOneByOne(topic: String): Task[List[Option[RecordMetadata]]] =
    Task.traverse((0 until size).toList)(i => producer.send(topic = topic_producer_1P_1RF, i))

  //syntax (P, P, RF) === (Parallelism factor, Partitions, Replication Factor)
  @Benchmark
  def monix_1P_1RF(): Unit = {
    val f = produceOneByOne(topic_producer_1P_1RF).runToFuture(singleThread)
    Await.ready(f, Duration.Inf)
  }

 @Benchmark
  def monix_2P_1RF(): Unit = {
   val f = produceOneByOne(topic_producer_2P_1RF).runToFuture(singleThread)
   Await.ready(f, Duration.Inf)
  }

}
