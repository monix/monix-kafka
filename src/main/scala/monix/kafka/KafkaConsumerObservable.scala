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

import monix.eval.{Callback, Task}
import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.kafka.config.ObservableCommitType
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, Observer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._
import scala.concurrent.{Future, blocking}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Consumes a Kafka stream. */
final class KafkaConsumerObservable[K,V] private
  (config: KafkaConsumerConfig, consumer: Task[KafkaConsumer[K,V]], io: Scheduler)
  extends Observable[ConsumerRecord[K,V]] {

  def unsafeSubscribeFn(out: Subscriber[ConsumerRecord[K,V]]): Cancelable = {
    val callback = new Callback[Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()
      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(out).runAsync(callback)(io)
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
          consumer.commitAsync()
      }

    /* Returns a task that continuously polls the `KafkaConsumer` for
     * new messages and feeds the given subscriber.
     *
     * Creates an asynchronous boundary on every poll.
     */
    def runLoop(consumer: KafkaConsumer[K,V]): Task[Unit] = {
      // Creates a task that polls the source, then feeds the downstream
      // subscriber, returning the resulting acknowledgement
      val ackTask: Task[Ack] = Task.unsafeCreate { (scheduler, conn, cb) =>
        implicit val s = scheduler

        // Forced asynchronous boundary
        s.executeAsync {
          val ackFuture =
            try consumer.synchronized {
              if (conn.isCanceled) Stop else {
                val next = consumer.poll(pollTimeoutMillis)
                if (shouldCommitBefore) consumerCommit(consumer)
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
                if (conn.isCanceled) {
                  streamErrors = false
                  cb.asyncOnSuccess(Stop)
                } else {
                  if (shouldCommitAfter) consumerCommit(consumer)
                  streamErrors = false
                  cb.asyncOnSuccess(ack)
                }
              } catch {
                case NonFatal(ex) =>
                  if (streamErrors) cb.asyncOnError(ex)
                  else s.reportFailure(ex)
              }

            case Failure(ex) =>
              cb.asyncOnError(ex)
          }
        }
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
      val cancelTask = Task {
        consumer.synchronized(consumer.close())
      }

      // By applying memoization, we are turning this
      // into an idempotent action, such that we are
      // guaranteed that consumer.close() happens
      // at most once
      cancelTask.memoize
    }

    Task.unsafeCreate { (s, conn, cb) =>
      val feedTask = consumer.flatMap { c =>
        // A task to execute on both cancellation and normal termination
        val onCancel = cancelTask(c)
        // We really need an easier way of adding
        // cancelable stuff to a task!
        conn.push(Cancelable(() => onCancel.runAsync(s)))
        runLoop(c).doOnFinish(_ => onCancel)
      }

      Task.unsafeStartNow(feedTask, s, conn, cb)
    }
  }
}

object KafkaConsumerObservable {
  /** Builds a [[KafkaConsumerObservable]] instance. */
  def apply[K,V](
    cfg: KafkaConsumerConfig,
    consumer: Task[KafkaConsumer[K,V]],
    io: Scheduler): KafkaConsumerObservable[K,V] =
    new KafkaConsumerObservable[K,V](cfg, consumer, io)


  /** Builds a [[KafkaConsumerObservable]] instance. */
  def apply[K,V](config: KafkaConsumerConfig, topics: List[String], io: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](config, topics, io)
    apply(config, consumer, io)
  }

  /** Returns a `Task` for creating a consumer instance. */
  def createConsumer[K,V](config: KafkaConsumerConfig, topics: List[String], io: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {

    import collection.JavaConverters._
    Task {
      val props = config.toProperties
      blocking {
        val consumer = new KafkaConsumer[K,V](props, K.create(), V.create())
        consumer.subscribe(topics.asJavaCollection)
        consumer
      }
    }
  }
}