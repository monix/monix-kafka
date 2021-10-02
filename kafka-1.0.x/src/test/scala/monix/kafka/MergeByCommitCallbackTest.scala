package monix.kafka

import cats.effect.Resource
import monix.eval.Task
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class MergeByCommitCallbackTest extends FunSuite with KafkaTestKit with ScalaCheckDrivenPropertyChecks with Matchers {

  val commitCallbacks: List[Commit] = List.fill(4)(new Commit {
    override def commitBatchSync(batch: Map[TopicPartition, Long]): Task[Unit] = Task.unit

    override def commitBatchAsync(batch: Map[TopicPartition, Long], callback: OffsetCommitCallback): Task[Unit] =
      Task.unit
  })

  val committableOffsetsGen: Gen[CommittableOffset] = for {
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    commit <- Gen.oneOf(commitCallbacks)
  } yield CommittableOffset(new TopicPartition("topic", partition), offset, commit)

  private def logic(bootstrapServer: String, topic: String) = {
    val kafkaConfig: KafkaConsumerConfig = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(bootstrapServer),
      groupId = "failing-logic",
      autoOffsetReset = AutoOffsetReset.Earliest
    )
    KafkaConsumerObservable
      .manualCommit[String, String](kafkaConfig, List(topic))
      .timeoutOnSlowUpstreamTo(6.seconds, Observable.empty)
      .foldLeft(CommittableOffsetBatch.empty) { case (batch, message) => batch.updated(message.committableOffset) }
      .map{completeBatch =>
        {Task.unit >> Task.sleep(3.second) >> Task.evalAsync(println("Committing async!!!")) >> completeBatch.commitAsync()}.runSyncUnsafe()
      }
      .headL
  }

  test("async commit finalizes successfully after cancellation") {
    EmbeddedKafka.start()
      val batchSize = 10

      val topicName = "random_topic"

      val producerCfg = KafkaProducerConfig.default.copy(
        bootstrapServers = List("127.0.0.1:6001"),
        clientId = "monix-kafka-producer-test"
      )

      val t = for {
          _ <- Resource.liftF(Task(KafkaProducer[String, String](producerCfg, io))).use { producer =>
            Task(producer.send(new ProducerRecord(topicName, "message1"))) >>
              Task(producer.send(new ProducerRecord(topicName, "message2")))
          }
          _ <- logic("127.0.0.1:6001", topicName)
        } yield ()
      t.runSyncUnsafe()

    EmbeddedKafka.stop()
  }

  private def createConsumer(i: Int, topicName: String): Observable[CommittableOffset] = {
    val cfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List("127.0.0.1:6001"),
      groupId = s"kafka-tests-$i",
      autoOffsetReset = AutoOffsetReset.Earliest
    )

    KafkaConsumerObservable
      .manualCommit[String, String](cfg, List(topicName))
      .executeOn(io)
      .map(_.committableOffset)
  }
}
