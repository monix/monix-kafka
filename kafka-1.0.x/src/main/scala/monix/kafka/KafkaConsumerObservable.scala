/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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
import monix.execution.cancelables.BooleanCancelable
import monix.execution.{Ack, Callback, Cancelable}
import monix.kafka.config.ObservableCommitType
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, Observer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, blocking}
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.util.{Failure, Success}

/** Exposes an `Observable` that consumes a Kafka stream by
  * means of a Kafka Consumer client.
  *
  * In order to get initialized, it needs a configuration. See the
  * [[KafkaConsumerConfig]] needed and see `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
final class KafkaConsumerObservable[K, V] private
  (config: KafkaConsumerConfig, consumer: Task[KafkaConsumer[K,V]])
  extends Observable[ConsumerRecord[K,V]] {

  def unsafeSubscribeFn(out: Subscriber[ConsumerRecord[K,V]]): Cancelable = {
    import out.scheduler

    val callback = new Callback[Throwable, Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()
      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(out).runAsync(callback)
  }

  private def feedTask(out: Subscriber[ConsumerRecord[K,V]]): Task[Unit] = {
    // Caching value to save CPU cycles
    val pollTimeoutMillis = config.fetchMaxWaitTime.toMillis
    // Boolean value indicating that we should trigger a commit before downstream ack
    val shouldCommitBefore = !config.enableAutoCommit && config.observableCommitOrder.isBefore
    // Boolean value indicating that we should trigger a commit after downstream ack
    val shouldCommitAfter = !config.enableAutoCommit && config.observableCommitOrder.isAfter

    /* Based on the [[KafkaConsumerConfig.observableCommitType]] it
     * chooses to commit the offsets in the consumer, by doing either
     * a `commitSync` or a `commitAsync`.
     *
     * MUST BE synchronized by `consumer`.
     */
    def consumerCommit(consumer: KafkaConsumer[K,V]): Unit =
      config.observableCommitType match {
        case ObservableCommitType.Sync =>
          blocking(consumer.commitSync())
        case ObservableCommitType.Async =>
          blocking(consumer.commitAsync())
      }

    /* Returns a task that continuously polls the `KafkaConsumer` for
     * new messages and feeds the given subscriber.
     *
     * Creates an asynchronous boundary on every poll.
     */
    def runLoop(consumer: KafkaConsumer[K,V]): Task[Unit] = {
      // Creates a task that polls the source, then feeds the downstream
      // subscriber, returning the resulting acknowledgement
      val ackTask: Task[Ack] = Task.create { (scheduler, cb) =>
        implicit val s = scheduler
        val asyncCb = Callback.forked(cb)
        val cancelable = BooleanCancelable()

        // Forced asynchronous boundary (on the I/O scheduler)
        s.executeAsync { () =>
          val ackFuture =
            try consumer.synchronized {
              if (cancelable.isCanceled) Stop else {
                val next = blocking(consumer.poll(pollTimeoutMillis))
                if (shouldCommitBefore) consumerCommit(consumer)
                // Feeding the observer happens on the Subscriber's scheduler
                // if any asynchronous boundaries happen
                Observer.feed(out, next.asScala)(out.scheduler)
              }
            } catch {
              case NonFatal(ex) =>
                Future.failed(ex)
            }

          ackFuture.syncOnComplete {
            case Success(ack) =>
              // The `streamError` flag protects against contract violations
              // (i.e. onSuccess/onError should happen only once).
              // Not really required, but we don't want to depend on the
              // scheduler implementation.
              var streamErrors = true
              try consumer.synchronized {
                // In case the task has been cancelled, there's no point
                // in continuing to do anything else
                if (cancelable.isCanceled) {
                  streamErrors = false
                  asyncCb.onSuccess(Stop)
                } else {
                  if (shouldCommitAfter) consumerCommit(consumer)
                  streamErrors = false
                  asyncCb.onSuccess(ack)
                }
              } catch {
                case NonFatal(ex) =>
                  if (streamErrors) asyncCb.onError(ex)
                  else s.reportFailure(ex)
              }

            case Failure(ex) =>
              asyncCb.onError(ex)
          }
        }
        cancelable
      }

      ackTask.flatMap {
        case Stop => Task.unit
        case Continue => runLoop(consumer)
      }
    }

    /* Returns a `Task` that triggers the closing of the
     * Kafka Consumer connection.
     */
    def cancelTask(consumer: KafkaConsumer[K,V]): Task[Unit] = {
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

    Task.create { (scheduler, cb) =>
      implicit val s = scheduler
      val feedTask = consumer.flatMap { c =>
        // Skipping all available messages on all partitions
        if (config.observableSeekToEndOnStart) c.seekToEnd(Nil.asJavaCollection)
        // A task to execute on both cancellation and normal termination
        val onCancel = cancelTask(c)
        runLoop(c).guarantee(onCancel)
      }
      feedTask.runAsync(cb)
    }
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
  def apply[K,V](
    cfg: KafkaConsumerConfig,
    consumer: Task[KafkaConsumer[K,V]]): KafkaConsumerObservable[K,V] =
    new KafkaConsumerObservable[K,V](cfg, consumer)

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def apply[K,V](cfg: KafkaConsumerConfig, topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](cfg, topics)
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
  def apply[K,V](cfg: KafkaConsumerConfig, topicsRegex: Regex)
                (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](cfg, topicsRegex)
    apply(cfg, consumer)
  }

  /** Returns a `Task` for creating a consumer instance given list of topics. */
  def createConsumer[K,V](config: KafkaConsumerConfig, topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {

    import collection.JavaConverters._
    Task.evalAsync {
      val props = config.toProperties
      blocking {
        val consumer = new KafkaConsumer[K,V](configMap, K.create(), V.create())
        consumer.subscribe(topics.asJava)
        consumer
      }
    }
  }

  /** Returns a `Task` for creating a consumer instance given topics regex. */
  def createConsumer[K,V](config: KafkaConsumerConfig, topicsRegex: Regex)
                         (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {
    Task.evalAsync {
      val props = config.toProperties
      blocking {
        val consumer = new KafkaConsumer[K,V](configMap, K.create(), V.create())
        consumer.subscribe(topicsRegex.pattern)
        consumer
      }
    }
  }
}