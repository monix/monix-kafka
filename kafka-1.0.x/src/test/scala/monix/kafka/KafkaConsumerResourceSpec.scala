package monix.kafka

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure

class KafkaConsumerResourceSpec extends FunSuite with KafkaTestKit with ScalaCheckDrivenPropertyChecks with Matchers {

  val consumerConf: KafkaConsumerConfig = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "monix-closeable-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-closeable-consumer-test"
  )
  
  test("async commit fails when observable was already cancelled") {

    withRunningKafka {
      val count = 11
      val topicName = "monix-closeable-consumer-test"

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](consumerConf, List(topicName))

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEvalF(producer.send)
        .completedL

      val listT = consumer
        .executeOn(io)
        .timeoutOnSlowUpstreamTo(6.seconds, Observable.empty)
        .foldLeft(CommittableOffsetBatch.empty) { case (batch, message) => batch.updated(message.committableOffset) }
        .mapEval(completeBatch => Task.sleep(3.second) >> completeBatch.commitAsync().as(completeBatch.offsets.size))
        .headL

       Task.parZip2(listT, pushT).attempt.map { result =>
         result.isLeft shouldBe true
         result.left.get.getMessage shouldBe "This consumer has already been closed."
       }
    }
  }

  test("consumer resource succeeds to commit even though observable was cancelled") {

    withRunningKafka {
      val count = 11
      val topicName = "monix-closeable-consumer-test"

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerResource.manualCommit[String, String](consumerConf, List(topicName))

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEvalF(producer.send)
        .completedL

      val listT = consumer.use { records =>
        records.executeOn(io)
          .timeoutOnSlowUpstreamTo(6.seconds, Observable.empty)
          .foldLeft(CommittableOffsetBatch.empty) { case (batch, message) => batch.updated(message.committableOffset) }
          .mapEval(completeBatch => Task.sleep(3.second) >> completeBatch.commitAsync().as(completeBatch.offsets.size))
          .headL
      }

      Task.parZip2(listT, pushT).attempt.map { result =>
        result.isRight shouldBe true
        result.map(_._1) shouldBe Right(count)
      }
    }
  }

}
