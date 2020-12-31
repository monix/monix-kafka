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
import monix.execution.Ack.Stop
import monix.execution.cancelables.BooleanCancelable
import monix.execution.{Ack, Callback}
import monix.reactive.Observer
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{Consumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.{blocking, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** KafkaConsumerObservable with ability to manual commit offsets
  * and forcibly disables auto commits in configuration.
  * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
  */
final class KafkaConsumerObservableManualCommit[K, V] private[kafka] (
  override protected val config: KafkaConsumerConfig,
  override protected val consumer: Task[Consumer[K, V]])
    extends KafkaConsumerObservable[K, V, CommittableMessage[K, V]] {

  // Caching value to save CPU cycles
  private val pollTimeoutMillis = config.fetchMaxWaitTime.toMillis

  case class CommitWithConsumer(consumer: Consumer[K, V]) extends Commit {

    override def commitBatchSync(batch: Map[TopicPartition, Long]): Task[Unit] =
      Task(blocking(consumer.synchronized(consumer.commitSync(batch.map {
        case (k, v) => k -> new OffsetAndMetadata(v)
      }.asJava))))

    override def commitBatchAsync(batch: Map[TopicPartition, Long], callback: OffsetCommitCallback): Task[Unit] =
      Task {
        blocking(consumer.synchronized(consumer.commitAsync(batch.map {
          case (k, v) => k -> new OffsetAndMetadata(v)
        }.asJava, callback)))
      }
  }

  override protected def ackTask(consumer: Consumer[K, V], out: Subscriber[CommittableMessage[K, V]]): Task[Ack] =
    Task.create { (scheduler, cb) =>
      implicit val s = scheduler
      val asyncCb = Callback.forked(cb)
      val cancelable = BooleanCancelable()

      val commit: Commit = CommitWithConsumer(consumer)

      // Forced asynchronous boundary (on the I/O scheduler)
      s.executeAsync { () =>
        val ackFuture =
          try consumer.synchronized {
            if (cancelable.isCanceled) Stop
            else {
              val next = blocking(consumer.poll(pollTimeoutMillis))
              // Feeding the observer happens on the Subscriber's scheduler
              // if any asynchronous boundaries happen
              val result = next.asScala.map { record =>
                CommittableMessage(
                  record,
                  CommittableOffset(
                    new TopicPartition(record.topic(), record.partition()),
                    record.offset() + 1,
                    commit))
              }
              Observer.feed(out, result)(out.scheduler)
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
            try {
              // In case the task has been cancelled, there's no point
              // in continuing to do anything else
              if (cancelable.isCanceled) {
                streamErrors = false
                asyncCb.onSuccess(Stop)
              } else {
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
}
