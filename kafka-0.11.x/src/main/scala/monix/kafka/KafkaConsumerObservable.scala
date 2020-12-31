/*
 * Copyright (c) 2014-2019 by The Monix Project Developers.
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
import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Ack, Callback, Cancelable}
import monix.kafka.config.ObservableCommitOrder
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.blocking
import scala.util.matching.Regex

/** Exposes an `Observable` that consumes a Kafka stream by
  * means of a Kafka Consumer client.
  *
  * In order to get initialized, it needs a configuration. See the
  * [[KafkaConsumerConfig]] needed and see `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
trait KafkaConsumerObservable[K, V, Out] extends Observable[Out] {
  protected def config: KafkaConsumerConfig
  protected def consumer: Task[Consumer[K, V]]

  /**
    * Creates a task that polls the source, then feeds the downstream
    * subscriber, returning the resulting acknowledgement
    * */
  protected def ackTask(consumer: Consumer[K, V], out: Subscriber[Out]): Task[Ack]

  override final def unsafeSubscribeFn(out: Subscriber[Out]): Cancelable = {
    import out.scheduler

    val callback = new Callback[Throwable, Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()
      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(out).runAsync(callback)
  }

  private def feedTask(out: Subscriber[Out]): Task[Unit] = {
    Task.create { (scheduler, cb) =>
      implicit val s = scheduler
      val feedTask = consumer.flatMap { c =>
        // Skipping all available messages on all partitions
        if (config.observableSeekOnStart.isSeekEnd) c.seekToEnd(Nil.asJavaCollection)
        else if (config.observableSeekOnStart.isSeekBeginning) c.seekToBeginning(Nil.asJavaCollection)
        // A task to execute on both cancellation and normal termination
        val onCancel = cancelTask(c)
        runLoop(c, out).guarantee(onCancel)
      }
      feedTask.runAsync(cb)
    }
  }

  /* Returns a task that continuously polls the `KafkaConsumer` for
   * new messages and feeds the given subscriber.
   *
   * Creates an asynchronous boundary on every poll.
   */
  private def runLoop(consumer: Consumer[K, V], out: Subscriber[Out]): Task[Unit] = {
    ackTask(consumer, out).flatMap {
      case Stop => Task.unit
      case Continue => runLoop(consumer, out)
    }
  }

  /* Returns a `Task` that triggers the closing of the
   * Kafka Consumer connection.
   */
  private def cancelTask(consumer: Consumer[K, V]): Task[Unit] = {
    // Forced asynchronous boundary
    val cancelTask = Task.evalAsync {
      consumer.synchronized(blocking(consumer.close()))
    }

    // By applying memoization, we are turning this
    // into an idempotent action, such that we are
    // guaranteed that consumer.close() happens
    // at most once
    cancelTask.memoize
  }
}

object KafkaConsumerObservable {

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param consumer is a factory for the
    *        `org.apache.kafka.clients.consumer.KafkaConsumer`
    *        instance to use for consuming from Kafka
    */
  def apply[K, V](
    cfg: KafkaConsumerConfig,
    consumer: Task[Consumer[K, V]]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] =
    new KafkaConsumerObservableAutoCommit[K, V](cfg, consumer)

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topics: List[String])(
    implicit K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topics)
    apply(cfg, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(
    implicit K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    apply(cfg, consumer)
  }

  /**
    * Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
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
    * */
  def manualCommit[K, V](
    cfg: KafkaConsumerConfig,
    consumer: Task[Consumer[K, V]]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val manualCommitConfig = cfg.copy(observableCommitOrder = ObservableCommitOrder.NoAck, enableAutoCommit = false)
    new KafkaConsumerObservableManualCommit[K, V](manualCommitConfig, consumer)
  }

  /**
    * Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
    * }}}
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used. Auto commit will disabled and
    *        observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    *
    * @param topics is the list of Kafka topics to subscribe to.
    * */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topics: List[String])(
    implicit K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topics)
    manualCommit(cfg, consumer)
  }

  /**
    * Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
    * }}}
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used. Auto commit will disabled and
    *        observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    *
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    * */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(
    implicit K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    manualCommit(cfg, consumer)
  }

  /** Returns a `Task` for creating a consumer instance given list of topics. */
  def createConsumer[K, V](config: KafkaConsumerConfig, topics: List[String])(
    implicit K: Deserializer[K],
    V: Deserializer[V]): Task[Consumer[K, V]] = {

    import collection.JavaConverters._
    Task.evalAsync {
      val configMap = config.toJavaMap
      blocking {
        val consumer = new KafkaConsumer[K, V](configMap, K.create(), V.create())
        consumer.subscribe(topics.asJava)
        consumer
      }
    }
  }

  /** Returns a `Task` for creating a consumer instance given topics regex. */
  def createConsumer[K, V](config: KafkaConsumerConfig, topicsRegex: Regex)(
    implicit K: Deserializer[K],
    V: Deserializer[V]): Task[Consumer[K, V]] = {
    Task.evalAsync {
      val configMap = config.toJavaMap
      blocking {
        val consumer = new KafkaConsumer[K, V](configMap, K.create(), V.create())
        consumer.subscribe(topicsRegex.pattern, new NoOpConsumerRebalanceListener)
        consumer
      }
    }
  }
}
