package monix.kafka

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Ack.Stop
import monix.execution.FutureUtils.extensions.FutureExtensions
import monix.execution.cancelables.SingleAssignmentCancelable
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.{Observable, Observer}
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer => ApacheKafkaConsumer}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

/** An `Observable` implementation that reads messages from `Kafka`.
  *
  * You need to call `addSubscription` in order for the Observable to start sending messages from that topic.
  * */
final class KafkaConsumer[K, V] private
  (config: KafkaConsumerConfig, ioScheduler: Scheduler)
  (implicit K: Deserializer[K], V: Deserializer[V])
  extends Observable[ConsumerRecord[K, V]] with StrictLogging {

  def unsafeSubscribeFn(subscriber: Subscriber[ConsumerRecord[K, V]]): Cancelable = {
    val source = underlying.onErrorHandleWith { ex =>
      logger.error(
        s"Unexpected error in KafkaConsumerObservable, retrying in ${config.retryBackoffTime}", ex)
      // Applying configured delay before retry!
      underlying.delaySubscription(config.retryBackoffTime)
    }

    source.unsafeSubscribeFn(subscriber)
  }

  def addTopicSubscription(topics: Set[String]): Unit = {
    logger.info(s"Subscribing to topics : ${topics.mkString(",")}")
    consumerRef.subscribe((currentSubscriptions() ++ topics).asJavaCollection)
  }

  /**
    *
    * @param topics the topics to be unsubscribed, if it is an `Set.empty` this method will unsubscribe from all topics.
    */
  def removeTopicSubscription(topics: Set[String]): Unit =
    if(topics.nonEmpty) {
      logger.info(s"Unsubscribing from topics : ${topics.mkString(",")}")
      consumerRef.subscribe((currentSubscriptions() -- topics).asJavaCollection)
    } else {
      logger.info(s"Unsubscribing from topics : ${currentSubscriptions().mkString(",")}")
      consumerRef.unsubscribe()
    }

  // Gets initialized on `subscribe`
  private lazy val consumerRef = {
    logger.info(s"Kafka consumer connecting to servers: ${config.servers.mkString(",")}")
    new ApacheKafkaConsumer[K,V](config.toProperties, K.create(), V.create())
  }

  private def currentSubscriptions() =
    consumerRef.subscription().asScala.toSet

  private lazy val underlying =
    Observable.unsafeCreate[ConsumerRecord[K, V]] { subscriber =>
      val cancelable = SingleAssignmentCancelable()

      // Forced asynchronous boundary, in order to not block
      // the current thread!
      ioScheduler.executeAsync {
        cancelable := Observable
          .fromStateAction[Set[String], ConsumerRecords[K, V]]{ subscriptions =>
            val pollRecords =
              if (subscriptions.nonEmpty)
                consumerRef.poll(config.fetchMaxWaitTime.toMillis)
              else
                ConsumerRecords.empty[K, V]()

            (pollRecords, currentSubscriptions())
          }(initialState = currentSubscriptions())
          .doOnSubscriptionCancel(consumerRef.close())
          .unsafeSubscribeFn(new Subscriber[ConsumerRecords[K, V]] {
            self =>

            implicit val scheduler = ioScheduler
            private[this] var isDone = false

            def onNext(elem: ConsumerRecords[K, V]): Future[Ack] =
              self.synchronized {
                if (isDone) Stop else {
                  val f = Observer.feed(subscriber, elem.asScala)

                  f.materialize.map {
                    case Success(ack) =>
                      if (!config.enableAutoCommit)
                        try {
                          consumerRef.commitSync()
                          ack
                        } catch {
                          case NonFatal(ex) =>
                            onError(ex)
                            Stop
                        }
                      else
                        ack
                    case Failure(ex) =>
                      onError(ex)
                      Stop
                  }
                }
              }

            def onError(ex: Throwable): Unit =
              self.synchronized {
                if (isDone) scheduler.reportFailure(ex) else {
                  isDone = true
                  subscriber.onError(ex)
                }
              }

            def onComplete(): Unit =
              self.synchronized {
                if (!isDone) {
                  isDone = true
                  subscriber.onComplete()
                }
              }
          })
      }

      cancelable
    }
}

object KafkaConsumer {
  def apply[K, V](config: KafkaConsumerConfig, ioScheduler: Scheduler): KafkaConsumer[K,V] =
    new KafkaConsumer(config, ioScheduler)
}
