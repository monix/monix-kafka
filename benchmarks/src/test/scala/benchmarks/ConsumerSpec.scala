package benchmarks

import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.kafka.{KafkaConsumerObservable, KafkaProducer}
import monix.kafka.benchmarks.MonixFixture
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ConsumerSpec extends FlatSpec with MonixFixture with Matchers with BeforeAndAfterAll {


  implicit lazy val io = Scheduler.io("monix-kafka-benchmark")

  override def beforeAll(): Unit = {
    super.beforeAll()
    produceGroupedSink(topic_consumer_1P_1RF, 1000, 1, 1).runSyncUnsafe()
  }

   s"Monix ${topic_consumer_1P_1RF}" should "exist" in {
     val producer = KafkaProducer[String, String](producerConf, global)
     val t = producer.send(topic = topic_consumer_1P_1RF, "test")

     t.runSyncUnsafe().isDefined shouldBe true
   }

   it should "allow " in {
     val conf = consumerConf.value()

     val f2 = KafkaConsumerObservable[Integer, Integer](conf, List(topic_consumer_1P_1RF)).take(1000).toListL.runToFuture(io)

     val elements = Await.result(f2, 10.seconds)
     elements.size shouldBe 1000
   }

}
