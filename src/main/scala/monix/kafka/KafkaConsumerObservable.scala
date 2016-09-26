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
import monix.execution.cancelables.{BooleanCancelable, CompositeCancelable, SingleAssignmentCancelable}
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, Observer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Consumes a Kafka stream. */
final class KafkaConsumerObservable[K,V] private
  (config: KafkaConsumerConfig, consumer: Task[KafkaConsumer[K,V]], io: Scheduler)
  extends Observable[ConsumerRecord[K,V]] {

  def unsafeSubscribeFn(out: Subscriber[ConsumerRecord[K,V]]): Cancelable = {
    import out.scheduler
    val consumerClose = SingleAssignmentCancelable()
    val composite = CompositeCancelable(consumerClose)

    val feedTask = consumer.flatMap { c =>
      consumerClose := Cancelable(() => cancelTask(c).runAsync(io))
      loop(c, out, composite).doOnFinish(_ => cancelTask(c))
    }

    composite += feedTask.runAsync(new Callback[Unit] {
      def onSuccess(value: Unit): Unit = ()
      def onError(ex: Throwable): Unit =
        scheduler.reportFailure(ex)
    })
  }

  private[this] val pollTimeoutMillis =
    config.fetchMaxWaitTime.toMillis

  /* Run-loop that continuously polls Kafka for new messages. */
  private def loop(consumer: KafkaConsumer[K,V], out: Subscriber[ConsumerRecord[K,V]],
    conn: BooleanCancelable): Task[Unit] = {

    val sendTask = Task.unsafeCreate[Ack] { (scheduler, conn, cb) =>
      implicit val s = scheduler
      io.executeAsync {
        val ack = consumer.synchronized {
          if (conn.isCanceled) Stop else
            try {
              val next = consumer.poll(pollTimeoutMillis)
              Observer.feed(out, next.asScala)
            } catch {
              case NonFatal(ex) =>
                Future.failed(ex)
            }
        }

        ack.syncOnComplete {
          case Success(value) =>
            cb.asyncOnSuccess(value)
          case Failure(ex) =>
            cb.asyncOnError(ex)
        }
      }
    }

    sendTask.flatMap {
      case Stop => Task.unit
      case Continue => loop(consumer, out, conn)
    }
  }

  private def cancelTask(consumer: KafkaConsumer[K,V]) = {
    val cancelTask = Task.unsafeCreate[Unit] { (s, conn, cb) =>
      io.executeAsync(consumer.synchronized {
        try consumer.close()
        catch { case NonFatal(ex) => s.reportFailure(ex) }
        cb.asyncOnSuccess(())(s)
      })
    }

    // By applying memoization, we are turning this
    // into an idempotent action, such that we are
    // guaranteed that consumer.close() happens
    // at most once
    cancelTask.memoize
  }
}

object KafkaConsumerObservable {
  /** Builds a [[KafkaConsumerObservable]] instance. */
  def apply[K,V](
    config: KafkaConsumerConfig,
    consumer: Task[KafkaConsumer[K,V]],
    io: Scheduler): KafkaConsumerObservable[K,V] =
    new KafkaConsumerObservable[K,V](config, consumer, io)

  /** Builds a [[KafkaConsumerObservable]] instance. */
  def apply[K,V](config: KafkaConsumerConfig, topics: List[String], io: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](config, topics, io)
    new KafkaConsumerObservable[K,V](config, consumer, io)
  }

  /** Returns a `Task` for creating a consumer instance. */
  def createConsumer[K,V](config: KafkaConsumerConfig, topics: List[String], io: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {

    import collection.JavaConverters._

    Task.unsafeCreate { (s, conn, cb) =>
      io.executeAsync {
        val props = config.toProperties
        try {
          val consumer = new KafkaConsumer[K,V](props, K.create(), V.create())
          consumer.subscribe(topics.asJavaCollection)
          cb.asyncOnSuccess(consumer)(s)
        }
        catch {
          case NonFatal(ex) =>
            cb.asyncOnError(ex)(s)
        }
      }
    }
  }
}