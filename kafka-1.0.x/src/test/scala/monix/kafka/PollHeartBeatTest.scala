package monix.kafka

import monix.eval.Task
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import monix.execution.Scheduler.Implicits.global
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class PollHeartBeatTest extends FunSuite with KafkaTestKit with ScalaFutures {

  val topicName = "monix-kafka-tests"
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 100.milliseconds)

  val producerCfg: KafkaProducerConfig = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-kafka-1-0-producer-test"
  )

  val consumerCfg: KafkaConsumerConfig = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "kafka-tests",
    clientId = "monix-kafka-1-0-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

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

      val (result, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe()
      assert(result.map(_.toInt).sum === (0 until count).sum)
    }
  }

  test("slow committable downstream with small poll heartbeat does not cause rebalancing") {
    withRunningKafka {

      val totalRecords = 100
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 200.millis
      val pollHeartbeat = 1.millis
      val maxPollInterval = 100.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = 200.millis).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalRecords)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync())
            .as(committableMessage)
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalRecords)
        .toListL

      val (committableMessages, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe()
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat * 10 < downstreamLatency)
      assert(pollHeartbeat < maxPollInterval)
      assert(maxPollInterval < downstreamLatency)
      assert((1 to totalRecords).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === totalRecords)
      assert(totalRecords === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }

  //unhappy scenario
  test("slow committable downstream with small `maxPollInterval` and high `pollHeartBeat` causes consumer rebalancing") {
    withRunningKafka {
      val totalRecords = 200
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 2.seconds
      val pollHeartbeat = 15.seconds
      val maxPollInterval = 100.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalRecords)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync())
            .as(committableMessage)
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalRecords)
        .toListL

      assert(pollHeartbeat > downstreamLatency)
      assert(maxPollInterval < downstreamLatency)
      assert(fastPollHeartbeatConfig.pollHeartbeatRate === pollHeartbeat)

      val t = Task.parZip2(listT.executeAsync, pushT.executeAsync).map(_._1)
      whenReady(t.runToFuture.failed) { ex =>
        assert(ex.getMessage.contains("the group has already rebalanced and assigned the partitions to another member"))
      }

    }
  }

  //java.lang.IllegalStateException: Received 50 unexpected messages.
  //at monix.kafka.KafkaConsumerObservable.$anonfun$pollHeartbeat$1(KafkaConsumerObservable.scala:112)
  test("slow committable downstream with high `maxPollInterval` and `pollHeartBeat` does not cause consumer rebalancing") {
    withRunningKafka {
      val count = 5
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 100.seconds
      val pollHeartbeat = 15.seconds
      val maxPollInterval = 10.seconds
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval).withPollHeartBeatRate(pollHeartbeat)

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
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync()).guarantee(Task.eval(println("Consumed message: " + committableMessage.record.value())))
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(count)
        .toListL

      val (committableMessages, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe()
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat < downstreamLatency)
      assert((1 to count).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === count)
      assert(count === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }


  test("2slows committable downstream with high `maxPollInterval` and `pollHeartBeat` does not cause consumer rebalancing") {
    withRunningKafka {
      val count = 10
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 100.millis
      val pollHeartbeat = 3000.millis
      val maxPollInterval = 50000.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync().guarantee(Task.eval(println("Consumed message: " + committableMessage.record.value()))))
            .as(committableMessage)
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(count)
        .toListL

      val (committableMessages, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe()
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat > downstreamLatency)
      assert(maxPollInterval > downstreamLatency)
      assert((1 to count).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === count)
      assert(count === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }
  test("slow downstream with long poll heart beat and smaller pollInterval causes rebalancing") {
    withRunningKafka {

      val fastMessages = 5
      val slowMessages = 3
      val totalMessages = fastMessages + slowMessages
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 4.seconds
      // the downstream latency of `slowMessages` is higher than the
      // `maxPollInterval` but smaller than `pollHeartBeat`,
      // kafka will trigger rebalancing and the consumer
      // will be kicked out of the consumer group.
      val pollHeartbeat = 1.seconds
      val maxPollInterval = 200.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[Integer, Integer](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[Integer, Integer](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalMessages)
        .map(Integer.valueOf)
        .map(msg => new ProducerRecord(topicName, msg, msg))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync().as(committableMessage))
          if(committableMessage.record.value() <= fastMessages) manualCommit
          else Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalMessages)
        .toListL

      val (committableMessages, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe()
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat > downstreamLatency)
      assert(committableMessages.map(_.record.value().toInt).sum === (1 to fastMessages).sum)
      assert(lastRecord.value().toInt === fastMessages)
      assert(lastCommittableOffset.offset === fastMessages)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }
}
