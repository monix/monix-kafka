package monix.kafka.benchmarks

//import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.kafka.KafkaProducer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import zio.ZLayer
import zio.kafka.consumer._
import zio.kafka.serde._
import zio.Runtime

//import scala.concurrent.Await
//import scala.concurrent.duration._

class ConsumerSpec extends FlatSpec with MonixFixture with Matchers with BeforeAndAfterAll {


  override def beforeAll(): Unit = {
    super.beforeAll()
  }
/*
   s"Monix topic" should "exist" in {
     val producer = KafkaProducer[String, String](producerConf, global)
     val t = producer.send(topic = monixTopic, "test")

     t.runSyncUnsafe().isDefined shouldBe true
   }

   it should "consume from monix topic" in {
     KafkaProducer[String, String](producerConf, global).send(topic = monixTopic, "test").runSyncUnsafe()

     val f = KafkaConsumerObservable[Integer, Integer](consumerConf.value(), List(monixTopic)).take(1).toListL.runToFuture(io)

     val elements = Await.result(f, 10.seconds)
     elements.size shouldBe 1
   }*/

  s"Zio topic" should "exist" in {
  // val producer = KafkaProducer[String, String](producerConf, global)
  // val t = producer.send(topic = zioTopic, "test")

  // t.runSyncUnsafe().isDefined shouldBe true
  }

  it should "consume from monix topic" in {

    val consumerSettings: ConsumerSettings = ConsumerSettings(List("localhost:9092")).withGroupId("group")

    val z = Consumer
      .subscribeAnd(Subscription.topics(monixTopic, zioTopic))
      .plainStream(Serde.string, Serde.string)
      .take(1)
      .provideSomeLayer(ZLayer.fromManaged(Consumer.make(consumerSettings)))
      .runLast

    KafkaProducer[String, String](producerConf, global).send(topic = monixTopic, "test").runSyncUnsafe()

    val r = Runtime.default.unsafeRun(z)
    r shouldBe "test"
  }

}
