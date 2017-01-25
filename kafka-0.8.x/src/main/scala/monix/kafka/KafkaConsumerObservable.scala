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

import kafka.consumer._
import kafka.message.MessageAndMetadata
import monix.eval.{Callback, Task}
import monix.execution.Cancelable
import monix.execution.cancelables.{BooleanCancelable, MultiAssignmentCancelable}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import scala.concurrent.blocking
import scala.util.control.NonFatal

/** Exposes an `Observable` that consumes a Kafka stream by
  * means of a Kafka Consumer client.
  *
  * In order to get initialized, it needs a configuration. See the
  * [[KafkaConsumerConfig]] needed and see `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
final class KafkaConsumerObservable[K, V] private
  (config: KafkaConsumerConfig, consumer: Task[ConsumerConnector], topicMap: Map[String, Int])
  (implicit K: Deserializer[K], V: Deserializer[V])
  extends Observable[MessageAndMetadata[K,V]] {

  def unsafeSubscribeFn(out: Subscriber[MessageAndMetadata[K, V]]): Cancelable = {
    import out.scheduler

    val conn = MultiAssignmentCancelable()
    val callback = new Callback[ConsumerConnector] {
      def onError(ex: Throwable): Unit =
        out.onError(ex)
      def onSuccess(value: ConsumerConnector): Unit =
        init(value, conn, out)
    }

    val c = consumer.runAsync(callback)
    conn.orderedUpdate(c, order=1)
  }

  private def init(
    connector: ConsumerConnector,
    conn: MultiAssignmentCancelable,
    out: Subscriber[MessageAndMetadata[K, V]]): Unit = {

    val streamsMap = connector.createMessageStreams(
      topicCountMap = topicMap,
      keyDecoder = K.create(),
      valueDecoder = V.create()
    )

    val streamsList = streamsMap.values.flatten.toList
    val rawStream = streamsList match {
      case Nil =>
        // Optimization for an empty sequence
        Observable.empty
      case s :: Nil =>
        // Optimization when subscribed to a single topic
        Observable.fromIterator(streamToIterator(s, conn))
      case list =>
        // Got a list of kafka streams, at least one per topic, so merge
        Observable.fromIterator(list.iterator).mergeMap { s =>
          Observable.fromIterator(streamToIterator(s, conn))
            // Each stream needs to start on its own thread, so fork
            .executeWithFork
        }
    }


    val connectorSubscription = Cancelable(() =>
      out.scheduler.executeAsync { () =>
        blocking {
          connector.shutdown()
        }
      })

    val cancelable = rawStream
      // In case of error or completion we need to close the connector
      .doOnTerminate(_ => connectorSubscription.cancel())
      // This might be problematic if the shutdown isn't thread-safe:
      .doOnSubscriptionCancel(() => connectorSubscription.cancel())
      // Ensuring we have an asynchronous boundary
      .executeWithFork
      .unsafeSubscribeFn(out)

    conn.orderedUpdate(cancelable, order = 2)
  }

  /** Wraps a `KafkaStream` into an `Iterator` implementation that uses
    * Scala's `blocking` context when calling `hasNext` and `next`,
    * thus making it possible to play well with Scala's `global`.
    */
  private def streamToIterator(source: KafkaStream[K,V], conn: BooleanCancelable): Iterator[MessageAndMetadata[K,V]] =
    new Iterator[MessageAndMetadata[K,V]] {
      private[this] val stream = source.iterator()

      def hasNext: Boolean = {
        // The hasNext operation is blocking until there is an element available
        try blocking(stream.hasNext()) catch {
          case NonFatal(ex) =>
            if (conn.isCanceled) {
              // If the connection was cancelled, ignore the error!
              false
            } else {
              throw ex
            }
        }
      }

      def next(): MessageAndMetadata[K, V] = {
        // Unfortunately we cannot catch errors here, however it should be
        // OK since `hasNext` is the method that advances to the next element
        blocking(stream.next())
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
    * @param topicsMap is the list of Kafka topics to subscribe to.
    *
    * @param connector is a factory for the `kafka.consumer.ConsumerConnector`
    *        instance to use for consuming from Kafka
    */
  def apply[K,V](
    cfg: KafkaConsumerConfig,
    connector: Task[ConsumerConnector],
    topicsMap: Map[String, Int])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] =
    new KafkaConsumerObservable[K,V](cfg, connector, topicsMap)

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topics is the list of Kafka topics to subscribe to.
    *
    * @param connector is a factory for the `kafka.consumer.ConsumerConnector`
    *        instance to use for consuming from Kafka
    */
  def apply[K,V](
    cfg: KafkaConsumerConfig,
    connector: Task[ConsumerConnector],
    topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] =
    new KafkaConsumerObservable[K,V](cfg, connector, topics.map(_ -> 1).toMap)

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topicsMap is the list of Kafka topics to subscribe to.
    */
  def apply[K,V](cfg: KafkaConsumerConfig, topicsMap: Map[String, Int])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConnector[K,V](cfg)
    apply(cfg, consumer, topicsMap)
  }

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg is the [[KafkaConsumerConfig]] needed for initializing the
    *        consumer; also make sure to see `monix/kafka/default.conf` for
    *        the default values being used.
    *
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def apply[K,V](cfg: KafkaConsumerConfig, topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] =
    apply(cfg, topics.map(_ -> 1).toMap)(K,V)

  /** Returns a `Task` for creating a consumer instance. */
  def createConnector[K,V](config: KafkaConsumerConfig)
    (implicit K: Deserializer[K], V: Deserializer[V]): Task[ConsumerConnector] = {

    Task {
      blocking {
        Consumer.create(new ConsumerConfig(config.toProperties))
      }
    }
  }
}