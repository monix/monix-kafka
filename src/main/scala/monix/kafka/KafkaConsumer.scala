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

/** An `Observable` implementation that reads messages from `Kafka`. */
final class KafkaConsumer[K, V] private
  (config: KafkaConsumerConfig, topic: String, ioScheduler: Scheduler)
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

  private val underlying =
    Observable.unsafeCreate[ConsumerRecord[K, V]] { subscriber =>
      val cancelable = SingleAssignmentCancelable()

      // Forced asynchronous boundary, in order to not block
      // the current thread!
      ioScheduler.executeAsync {
        val configProps = config.toProperties

        logger.info(s"Kafka Consumer connecting with $configProps")

        val consumer = new ApacheKafkaConsumer[K, V](configProps, K.create(), V.create())

        consumer.subscribe(List(topic).asJavaCollection)
        logger.info(s"Subscribing to topic : $topic")

        cancelable := Observable
          .repeatEval(consumer.poll(config.fetchMaxWaitTime.toMillis))
          .doOnSubscriptionCancel(consumer.close())
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
                          consumer.commitSync()
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
  def apply[K, V](config: KafkaConsumerConfig, ioScheduler: Scheduler)(topic: String): KafkaConsumer[K,V] =
    new KafkaConsumer(config, topic, ioScheduler)
}
