/*
 * Copyright (c) 2014-2016 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monixio/monix-kafka
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
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

class MonixKafkaTest extends FunSuite {
  val topicName = "monix-kafka-tests"

  test("full producer/consumer test") {
    val count = 10000
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List("127.0.0.1:9092")
    )

    val consumerCfg = KafkaConsumerConfig.default.copy(
      zookeeperConnect = "127.0.0.1:2181",
      groupId = "kafka-tests",
      autoOffsetReset = AutoOffsetReset.Smallest
    )

    val producer = KafkaProducerSink[String,String](producerCfg, io)
    val consumer = KafkaConsumerObservable[String,String](consumerCfg, List(topicName)).executeOn(io)

    val pushT = Observable.range(0, count)
      .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
      .bufferIntrospective(1024)
      .consumeWith(producer)

    val listT = consumer
      .take(count)
      .map(_.message())
      .toListL

    val (result, _) = Await.result(Task.zip2(Task.fork(listT), Task.fork(pushT)).runAsync, 5.minutes)
    assert(result.map(_.toInt).sum === (0 until count).sum)
  }
}
