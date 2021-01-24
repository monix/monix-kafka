/*
 * Copyright (c) 2014-2019 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monix/monix-kafka
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.kafka

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class MonixKafkaTopicListTest extends FunSuite with KafkaTestKit {
  val topicName = "monix-kafka-tests"

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-kafka-1-0-producer-test"
  )

  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "kafka-tests",
    clientId = "monix-kafka-1-0-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

  test("publish one message when subscribed to topics list") {

    withRunningKafka {
      val producer = KafkaProducer[String, String](producerCfg, io)

      val consumerTask =
        KafkaConsumerObservable.createConsumer[String, String](consumerCfg, List(topicName)).executeOn(io)
      val consumer = Await.result(consumerTask.runToFuture, 60.seconds)

      try {
        // Publishing one message
        val send = producer.send(topicName, "my-message")
        Await.result(send.runToFuture, 30.seconds)

        val records = consumer.poll(10.seconds.toMillis).asScala.map(_.value()).toList
        assert(records === List("my-message"))
      } finally {
        Await.result(producer.close().runToFuture, Duration.Inf)
        consumer.close()
      }
    }
  }

  test("listen for one message when subscribed to topics list") {
    withRunningKafka {
      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerCfg, List(topicName)).executeOn(io)
      try {
        // Publishing one message
        val send = producer.send(topicName, "test-message")
        Await.result(send.runToFuture, 30.seconds)

        val first = consumer.take(1).map(_.value()).firstL
        val result = Await.result(first.runToFuture, 30.seconds)
        assert(result === "test-message")
      } finally {
        Await.result(producer.close().runToFuture, Duration.Inf)
      }
    }
  }

  test("full producer/consumer test when subscribed to topics list") {
    withRunningKafka {
      val count = 10000

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerCfg, List(topicName)).executeOn(io).take(count)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .map(_.value())
        .toListL

      val (result, _) = Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)
      assert(result.map(_.toInt).sum === (0 until count).sum)
    }
  }

  test("manual commit consumer test when subscribed to topics list") {
    withRunningKafka {

      val count = 10000
      val topicName = "monix-kafka-manual-commit-tests"

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](consumerCfg, List(topicName))

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .executeOn(io)
        .bufferTumbling(count)
        .map { messages => messages.map(_.record.value()) -> CommittableOffsetBatch(messages.map(_.committableOffset)) }
        .mapEval { case (values, batch) => Task.shift *> batch.commitSync().map(_ => values -> batch.offsets) }
        .headL

      val ((result, offsets), _) =
        Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)

      val properOffsets = Map(new TopicPartition(topicName, 0) -> 10000)
      assert(result.map(_.toInt).sum === (0 until count).sum && offsets === properOffsets)
    }
  }

  test("manual async commit consumer test when subscribed to topics list") {
    withRunningKafka {

      val count = 10000
      val topicName = "monix-kafka-manual-commit-tests"

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](consumerCfg, List(topicName))

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .executeOn(io)
        .bufferTumbling(count)
        .map { messages =>
          messages.map(_.record.value()) -> CommittableOffsetBatch(messages.map(_.committableOffset))
        }
        .mapEval { case (values, batch) => Task.shift *> batch.commitAsync().map(_ => values -> batch.offsets) }
        .headL

      val ((result, offsets), _) =
        Await.result(Task.parZip2(listT.executeAsync, pushT.executeAsync).runToFuture, 60.seconds)

      val properOffsets = Map(new TopicPartition(topicName, 0) -> 10000)
      assert(result.map(_.toInt).sum === (0 until count).sum && offsets === properOffsets)
    }
  }

  test("publish to closed producer when subscribed to topics list") {
    withRunningKafka {
      val producer = KafkaProducer[String, String](producerCfg, io)
      val sendTask = producer.send(topicName, "test-message")

      val result = for {
        //Force creation of producer
        s1 <- producer.send(topicName, "test-message-1")
        res <- Task.parZip2(producer.close(), Task.parSequence(List.fill(10)(sendTask)).attempt)
        (_, s2) = res
        s3 <- sendTask
      } yield (s1, s2, s3)

      val (first, second, third) = Await.result(result.runToFuture, 60.seconds)
      assert(first.isDefined && second.isRight && third.isEmpty)
    }
  }

  test("slow batches processing doesn't cause rebalancing") {
    withRunningKafka {
      val count = 10000

      val consumerConfig = consumerCfg.copy(
        maxPollInterval = 200.millis,
        heartbeatInterval = 10.millis
      )

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerConfig, List(topicName)).executeOn(io)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .take(count)
        .map(_.value())
        .bufferTumbling(count / 4)
        .mapEval(s => Task.sleep(2.second) >> Task.delay(s))
        .flatMap(Observable.fromIterable)
        .toListL

      val (result, _) = Await.result(Task.parZip2(listT.executeAsync, pushT.executeAsync).runToFuture, 60.seconds)
      assert(result.map(_.toInt).sum === (0 until count).sum)
    }
  }

  test("slow upstream with manual async commit commit doesn't cause rebalancing") {
    withRunningKafka {

      val count = 200
      val topicName = "monix-kafka-manual-commit-tests"
      val delay = 200.millis
      val pollHeartbeat = 2.millis
      val fastPollHeartbeatConfig = consumerCfg.copy(
        maxPollInterval = 200.millis,
        observablePollHeartbeatRate = pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .doOnNextF { committableMessage =>
          Task.sleep(delay) *>
           Task.shift >> CommittableOffsetBatch(Seq(committableMessage.committableOffset)).commitAsync().executeAsync
        }
        .take(count)
        .toListL

      val (committableMessages, _) =
        Await.result(Task.parZip2(listT.executeAsync, pushT.executeAsync).runToFuture, 100.seconds)
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat * 10 < delay)
      assert((1 to count).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === count)
      assert(count === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition )
    }
  }
}
