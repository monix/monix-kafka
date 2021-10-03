/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
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

import cats.effect.Resource
import monix.eval.Task
import monix.kafka.KafkaConsumerObservable.createConsumer
import monix.kafka.config.ObservableCommitOrder
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}

import scala.concurrent.blocking
import scala.util.matching.Regex

/** Acquires and releases a [[KafkaConsumer]] within a [[Resource]]
  * is exposed in form of [[KafkaConsumerObservable]], which consumes
  * and emits records from the specified topic.
  *
  * In order to get initialized, it needs a configuration.
  * @see the [[KafkaConsumerConfig]] needed and `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
object KafkaConsumerResource {


  /** A [[Resource]] that acquires a [[KafkaConsumer]] used
    * to build a [[KafkaConsumerObservableAutoCommit]] instance,
    * that will be released after it's usage.
    *
    * @note The consumer will act consequently depending on the
    * [[ObservableCommitOrder]] that was chosen from configuration.
    * Which can be configured from the key `monix.observable.commit.order`.
    *
    * @param cfg      is the [[KafkaConsumerConfig]] needed for initializing the
    *                 consumer; also make sure to see `monix/kafka/default.conf` for
    *                 the default values being used.
    * @param consumer is a factory for the
    *                 `org.apache.kafka.clients.consumer.KafkaConsumer`
    *                 instance to use for consuming from Kafka
    */
  def apply[K, V](
                        cfg: KafkaConsumerConfig,
                        consumer: Task[Consumer[K, V]]): Resource[Task, KafkaConsumerObservable[K, V, ConsumerRecord[K, V]]]= {
    for {
      consumer <- Resource.make(consumer){ consumer =>
        Task.evalAsync(consumer.synchronized{ blocking(consumer.close())})
      }
      consumerObservable <- Resource.liftF(Task(new KafkaConsumerObservableAutoCommit[K, V](cfg, Task.pure(consumer), shouldClose = false)))
    } yield consumerObservable
  }

  /** A [[Resource]] that acquires a [[KafkaConsumer]] used
    * to build a [[KafkaConsumerObservableAutoCommit]] instance,
    * that will be released after it's usage.
    *
    * @note The consumer will act consequently depending on the
    * [[ObservableCommitOrder]] that was chosen from configuration.
    * Which can be configured from the key `monix.observable.commit.order`.
    *
    * @param cfg    is the [[KafkaConsumerConfig]] needed for initializing the
    *               consumer; also make sure to see `monix/kafka/default.conf` for
    *               the default values being used.
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topics: List[String])(implicit
                                                                       K: Deserializer[K],
                                                                       V: Deserializer[V]): Resource[Task, KafkaConsumerObservable[K, V, ConsumerRecord[K, V]]] = {
    val consumer = createConsumer[K, V](cfg, topics)
    apply(cfg, consumer)
  }

  /** A [[Resource]] that acquires a [[KafkaConsumer]] used
    * to build a [[KafkaConsumerObservableAutoCommit]] instance,
    * that will be released after it's usage.
    *
    * @note The consumer will act consequently depending on the
    * [[ObservableCommitOrder]] that was chosen from configuration.
    * Which can be configured from the key `monix.observable.commit.order`.
    *
    * @param cfg         is the [[KafkaConsumerConfig]] needed for initializing the
    *                    consumer; also make sure to see `monix/kafka/default.conf` for
    *                    the default values being used.
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(implicit
                                                                     K: Deserializer[K],
                                                                     V: Deserializer[V]): Resource[Task, KafkaConsumerObservable[K, V, ConsumerRecord[K, V]]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    apply(cfg, consumer)
  }

  /**
    * A [[Resource]] that acquires a [[KafkaConsumer]] used
    * to build a [[KafkaConsumerObservableManualCommit]] instance,
    * which provides the ability to manual commit offsets and
    * forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's [[ConsumerRecord]].
    *
    * ==Example==
    * {{{
    *   KafkaConsumerResource.manualCommit[String,String](consumerCfg, List(topicName)).use{ committableMessages =>
    *     committableMessages.map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitAsync())
    *     .completedL
    * }}}
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used. Auto commit will disabled and
    *        observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    *
    * @param consumer is a factory for the
    *        `org.apache.kafka.clients.consumer.KafkaConsumer`
    *        instance to use for consuming from Kafka
    */
  def manualCommit[K, V](
                          cfg: KafkaConsumerConfig,
                          consumer: Task[Consumer[K, V]]): Resource[Task, KafkaConsumerObservable[K, V, CommittableMessage[K, V]]] = {
    for {
      consumer <- Resource.make(consumer){ consumer =>
        Task.evalAsync(consumer.synchronized{ blocking(consumer.close())})
      }
      manualCommitConf = cfg.copy(observableCommitOrder = ObservableCommitOrder.NoAck, enableAutoCommit = false)
      consumerObservable <- Resource.liftF(Task(new KafkaConsumerObservableManualCommit[K, V](manualCommitConf, Task.pure(consumer), shouldClose = false)))
    } yield consumerObservable
  }

  /** Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * ==Example==
    * {{{
    *   KafkaConsumerResource.manualCommit[String,String](consumerCfg, List(topicName)).use{ committableMessages =>
    *     committableMessages.map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .completedL
    *   }
    * }}}
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used. Auto commit will disabled and
    *        observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    *
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topics: List[String])(implicit
                                                                          K: Deserializer[K],
                                                                          V: Deserializer[V]): Resource[Task, KafkaConsumerObservable[K, V, CommittableMessage[K, V]]] = {

    val consumer = createConsumer[K, V](cfg, topics)
    manualCommit(cfg, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * ==Example==
    * {{{
    *   KafkaConsumerResource.manualCommit[String,String](consumerCfg, List(topicName)).use{ committableMessages =>
    *     committableMessages.map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .completedL
    *  }
    * }}}
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used. Auto commit will disabled and
    *        observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    *
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(implicit
                                                                        K: Deserializer[K],
                                                                        V: Deserializer[V]): Resource[Task, KafkaConsumerObservable[K, V, CommittableMessage[K, V]]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    manualCommit(cfg, consumer)
  }

}