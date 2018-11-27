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

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.atomic.Atomic
import monix.execution.cancelables.{SingleAssignCancelable, StackedCancelable}
import monix.execution.{Callback, Cancelable, Scheduler}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata, Callback => KafkaCallback, KafkaProducer => ApacheKafkaProducer}

import scala.util.control.NonFatal

/**
  * Wraps the Kafka Producer.
  *
  * Calling `producer.send` returns a `Task[Option[RecordMetadata]]`
  * which can then be run and transformed into a `Future`.
  *
  * If the `Task` completes with `None` it means that `producer.send`
  * method was called after the producer was closed and that
  * the message wasn't successfully acknowledged by the Kafka broker.
  * In case of the failure of the underlying Kafka client the producer
  * will bubble up the exception and fail the `Task`.
  *
  * All successfully delivered messages will complete with `Some[RecordMetadata]`.
  */
trait KafkaProducer[K,V] extends Serializable {
  def underlying: Task[ApacheKafkaProducer[K,V]]
  def send(topic: String, value: V): Task[Option[RecordMetadata]]
  def send(topic: String, key: K, value: V): Task[Option[RecordMetadata]]
  def send(record: ProducerRecord[K,V]): Task[Option[RecordMetadata]]
  def close(): Task[Unit]
}

object KafkaProducer {
  /** Builds a [[KafkaProducer]] instance. */
  def apply[K,V](config: KafkaProducerConfig, sc: Scheduler)
    (implicit K: Serializer[K], V: Serializer[V]): KafkaProducer[K,V] =
    new Implementation[K,V](config, sc)

  private final class Implementation[K,V](config: KafkaProducerConfig, sc: Scheduler)
    (implicit K: Serializer[K], V: Serializer[V])
    extends KafkaProducer[K,V] with StrictLogging {

    // Alias needed for being precise when blocking
    self =>

    // MUST BE synchronized by `self`
    private[this] var isCanceled = false

    // Gets initialized on the first `send`
    private lazy val producerRef = {
      logger.info(s"Kafka producer connecting to servers: ${config.bootstrapServers.mkString(",")}")
      new ApacheKafkaProducer[K,V](config.toJavaMap, K.create(), V.create())
    }

    def underlying: Task[ApacheKafkaProducer[K, V]] =
      Task.eval(producerRef)

    def send(topic: String, value: V): Task[Option[RecordMetadata]] =
      send(new ProducerRecord[K,V](topic, value))

    def send(topic: String, key: K, value: V): Task[Option[RecordMetadata]] =
      send(new ProducerRecord[K,V](topic, key, value))

    def send(record: ProducerRecord[K,V]): Task[Option[RecordMetadata]] =
      Task.create[Option[RecordMetadata]] { (s, cb) =>
        val asyncCb = Callback.forked(cb)(s)
        val connection = StackedCancelable()
        // Forcing asynchronous boundary
        sc.executeAsync(() => self.synchronized {
          if (isCanceled) {
            asyncCb.onSuccess(None)
          }
          else {
            val isActive = Atomic(true)
            val cancelable = SingleAssignCancelable()
            try {
              // Force evaluation
              val producer = producerRef

              // Using asynchronous API
              val future = producer.send(record, new KafkaCallback {
                def onCompletion(meta: RecordMetadata, exception: Exception): Unit =
                  if (isActive.getAndSet(false)) {
                    connection.pop()
                    if (exception != null)
                      asyncCb.onError(exception)
                    else
                      asyncCb.onSuccess(Option(meta))
                  }
                  else if (exception != null) {
                    s.reportFailure(exception)
                  }
              })

              cancelable := Cancelable(() => future.cancel(false))
            }
            catch {
              case NonFatal(ex) =>
                // Needs synchronization, otherwise we are violating the contract
                if (isActive.compareAndSet(expect = true, update = false)) {
                  connection.pop()
                  asyncCb.onError(ex)
                } else {
                  s.reportFailure(ex)
                }
            }
          }
        })
        connection
      }

    def close(): Task[Unit] =
      Task.create { (s, cb) =>
        val asyncCb = Callback.forked(cb)(s)
        // Forcing asynchronous boundary
        sc.executeAsync { () =>
          self.synchronized {
            if (isCanceled) {
              asyncCb.onSuccess(())
            }
            else {
              isCanceled = true
              try {
                producerRef.close()
                asyncCb.onSuccess(())
              } catch {
                case NonFatal(ex) =>
                  asyncCb.onError(ex)
              }
            }
          }
        }
      }
  }
}
