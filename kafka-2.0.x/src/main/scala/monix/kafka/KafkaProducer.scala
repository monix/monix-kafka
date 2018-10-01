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
import monix.execution.cancelables.SingleAssignCancelable
import monix.execution.{Cancelable, Scheduler}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata, KafkaProducer => ApacheKafkaProducer}

import scala.util.control.NonFatal

/** Wraps the Kafka Producer. */
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
      new ApacheKafkaProducer[K,V](config.toProperties, K.create(), V.create())
    }

    def underlying: Task[ApacheKafkaProducer[K, V]] =
      Task.eval(producerRef)

    def send(topic: String, value: V): Task[Option[RecordMetadata]] =
      send(new ProducerRecord[K,V](topic, value))

    def send(topic: String, key: K, value: V): Task[Option[RecordMetadata]] =
      send(new ProducerRecord[K,V](topic, key, value))

    def send(record: ProducerRecord[K,V]): Task[Option[RecordMetadata]] =
      Task.unsafeCreate[Option[RecordMetadata]] { (context, cb) =>
        // Forcing asynchronous boundary
        sc.executeAsync(() => self.synchronized {
          val s = context.scheduler
          if (isCanceled) {
            cb.asyncOnSuccess(None)(s)
          }
          else {
            val isActive = Atomic(true)
            val cancelable = SingleAssignCancelable()
            context.connection.push(cancelable)

            try {
              // Force evaluation
              val producer = producerRef

              // Using asynchronous API
              val future = producer.send(record, new Callback {
                def onCompletion(meta: RecordMetadata, exception: Exception): Unit =
                  if (isActive.getAndSet(false)) {
                    context.connection.pop()
                    if (exception != null)
                      cb.asyncOnError(exception)(s)
                    else
                      cb.asyncOnSuccess(Option(meta))(s)
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
                  context.connection.pop()
                  cb.asyncOnError(ex)(s)
                } else {
                  s.reportFailure(ex)
                }
            }
          }
        })
      }

    def close(): Task[Unit] =
      Task.unsafeCreate { (context, cb) =>
        // Forcing asynchronous boundary
        sc.executeAsync { () =>
          self.synchronized {
            val s = context.scheduler
            if (isCanceled) {
              cb.asyncOnSuccess(())(s)
            }
            else {
              isCanceled = true
              try {
                producerRef.close()
                cb.asyncOnSuccess(())(s)
              } catch {
                case NonFatal(ex) =>
                  cb.asyncOnError(ex)(s)
              }
            }
          }
        }
      }
  }
}
