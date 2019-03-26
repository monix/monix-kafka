package monix.kafka

import java.time.Instant

import monix.eval.Task
import monix.kafka.config.{AutoOffsetReset, StartFrom}
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Promise}
//todo fix Error while fetching metadata with correlation id 1 : {kafka-consumer-test=LEADER_NOT_AVAILABLE}
class KafkaConsumerObservableTest extends FunSuite with KafkaTestKit {
  val topicName = "kafka-consumer-test"

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-kafka-1-0-producer-test"
  )

  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "kafka-consumer-tests",
    clientId = "monix-kafka-1-0-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

  var recordMetadata = Promise[Seq[RecordMetadata]]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val producer = KafkaProducer[String, String](producerCfg, io)
    try {
      val send = Task.sequence(
        (0 to 9)
          .map(_.toString)
          .map(producer.send(topicName, _)))
      val value = send.runToFuture(io)
      Await.result(value, 30.seconds)
      recordMetadata.completeWith(value.map(_.flatten)(io))
    } finally {
      Await.result(producer.close().runToFuture(io), Duration.Inf)
    }
  }

  implicit val ec = io

  test("consumer should start from earliest") {
    val metadata = Await.result(recordMetadata.future, 20.seconds)

    val conf = consumerCfg.copy(observableStartFrom = StartFrom.FromEarliest)
    val consumer = KafkaConsumerObservable[String, String](conf, List(topicName)).executeOn(io)
    val messages = consumer.toListL.runToFuture
    val result = Await.result(messages, 20.seconds)
    assert(result.length == 10)

  }

  test("consumer should start from latest") {
    val metadata = Await.result(recordMetadata.future, 20.seconds)
    val conf = consumerCfg.copy(observableStartFrom = StartFrom.FromLatest)
    val consumer = KafkaConsumerObservable[String, String](conf, List(topicName)).executeOn(io)

    val messages = consumer.toListL.runToFuture
    val result = Await.result(messages, 20.seconds)
    assert(result.isEmpty)
  }

  test("consumer should start from committed") {
    val metadata = Await.result(recordMetadata.future, 20.seconds)
    val conf = consumerCfg.copy(
      observableStartFrom = StartFrom.FromCommitted,
      groupId = "kafka-consumer-tests-committed"
    )
    val c = Await
      .result(KafkaConsumerObservable.createConsumer[String, String](conf, List(topicName)).runToFuture, 10.seconds)

    val partitionsAtTime = c.assignment().asScala.map(tp => tp -> Long.box(metadata(5).timestamp())).toMap
    c.offsetsForTimes(partitionsAtTime.asJava)

    val consumer = KafkaConsumerObservable[String, String](conf, List(topicName)).executeOn(io)
    val messages = consumer.toListL.runToFuture
    val result = Await.result(messages, 20.seconds)
    assert(result.length == 5)
  }

  test("consumer should start from certain timestamp") {
    val metadata = Await.result(recordMetadata.future, 20.seconds)
    val conf = consumerCfg.copy(
      observableStartFrom = StartFrom.StartAt(Instant.ofEpochMilli(metadata(7).timestamp())),
      groupId = "kafka-consumer-tests"
    )
    val consumer = KafkaConsumerObservable[String, String](conf, List(topicName)).executeOn(io)

    val messages = consumer.toListL.runToFuture
    val result = Await.result(messages, 20.seconds)
    assert(result.length == 3)
  }
}
