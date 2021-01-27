package monix.kafka.benchmarks

import cats.effect.IO
import fs2.kafka.{KafkaConsumer, KafkaProducer, ProducerRecord, ProducerRecords, commitBatchWithin}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import cats.effect._
import cats.implicits._

//import monix.execution.Scheduler
//import monix.execution.Scheduler.Implicits.global
//import monix.kafka.KafkaProducer
//import zio.ZLayer
//import zio.kafka.consumer._
//import zio.kafka.serde._
//import zio.Runtime
//
//import fs2.kafka._

//import scala.concurrent.Await
import scala.concurrent.duration._

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

 /* it should "consume from monix topic" in {

    val consumerSettings: ConsumerSettings = ConsumerSettings(List("127.0.0.1:9092")).withGroupId("groupId")
      .withClientId("client")

    val z = Consumer
      .subscribeAnd(Subscription.topics(monixTopic, zioTopic))
      .plainStream(Serde.string, Serde.string)
      .take(1)
      .provideSomeLayer(ZLayer.fromManaged(Consumer.make(consumerSettings)))
      .runHead

    val r = Runtime.default.unsafeRunTask(z)


    KafkaProducer[String, String](producerConf, global).send(topic = monixTopic, "zio-test").runSyncUnsafe()

    r shouldBe "zio-test"
  }*/
  import cats.effect._
  import cats.implicits._
  import fs2.kafka._
  import scala.concurrent.duration._

  val stream =
    KafkaConsumer.stream(fs2ConsumerSettings)
      .evalTap(_.subscribeTo("topic"))
      .flatMap(_.stream)
      .mapAsync(25) { committable =>
        IO.pure(committable.record -> committable.record.value)
          .map { case (key, value) =>
            val record = ProducerRecord("topic", key, value)
            ProducerRecords.one(record, committable.offset)
          }
      }
      .through(KafkaProducer.pipe(fs2ProducerSettings))
      .map(_.passthrough)
      .through(commitBatchWithin(500, 15.seconds))
      .compile

}
