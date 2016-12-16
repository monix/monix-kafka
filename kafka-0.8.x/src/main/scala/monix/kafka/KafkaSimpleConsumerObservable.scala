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

import monix.reactive.Observable
import monix.execution.Scheduler
import kafka.consumer._
import kafka.message.MessageAndMetadata

import scala.collection.Map

object KafkaSimpleConsumerObservable {

  def streams[K, V](cfg: KafkaConsumerConfig, topicMap: Map[String, Int], io: Scheduler)
                   (implicit K: Decoder[K], V: Decoder[V]): Seq[Observable[MessageAndMetadata[K, V]]] =
    Consumer
      .create(new ConsumerConfig(cfg.toProperties))
      .createMessageStreams(
        topicCountMap = topicMap,
        keyDecoder = K.create(),
        valueDecoder = V.create()
      ).values.toSeq.flatten
      .map(Observable
        .fromIterable(_)
        .subscribeOn(io)
      )

}