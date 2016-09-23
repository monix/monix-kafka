package monix.kafka

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Ack.Stop
import monix.execution.FutureUtils.extensions.FutureExtensions
import monix.execution.cancelables.SingleAssignmentCancelable
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer => ApacheKafkaConsumer}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.collection.JavaConversions.{asScalaIterator, seqAsJavaList}

/** An `Observable` implementation that reads messages from `Kafka`. */
final class KafkaConsumer[K, V] private
  (config: KafkaConsumerConfig, topic: String, ioScheduler: Scheduler)
  (implicit K: Deserializer[K], V: Deserializer[V])
  extends Observable[(K, V)] with StrictLogging {

  def unsafeSubscribeFn(subscriber: Subscriber[(K, V)]): Cancelable = {
    val source = underlying.onErrorHandleWith { ex =>
      logger.error(
        s"Unexpected error in KafkaConsumerObservable, retrying in ${config.retryBackoffTime}", ex)
      // Applying configured delay before retry!
      underlying.delaySubscription(config.retryBackoffTime)
    }

    source.unsafeSubscribeFn(subscriber)
  }

  private val underlying =
    Observable.unsafeCreate[(K, V)] { subscriber =>
      val cancelable = SingleAssignmentCancelable()

      // Forced asynchronous boundary, in order to not block
      // the current thread!
      ioScheduler.executeAsync {
        val configProps = config.toProperties

        logger.info(s"Kafka Consumer connecting with $configProps")

        val consumer = new ApacheKafkaConsumer[K, V](configProps, K.create(), V.create())

        consumer.subscribe(List(topic))
        logger.info(s"Subscribing to topic : $topic")

        cancelable := Observable
          .fromIterator(consumer.poll(config.fetchMaxWaitTime.toMillis).iterator(), () => consumer.close())
          .unsafeSubscribeFn(new Subscriber[ConsumerRecord[K, V]] {
            self =>

            implicit val scheduler = ioScheduler
            private[this] var isDone = false

            def onNext(elem: ConsumerRecord[K, V]): Future[Ack] =
              self.synchronized {
                if (isDone) Stop else {
                  val f = subscriber.onNext((elem.key(), elem.value()))

                  f.materialize.map {
                    case Success(ack) =>
                      try {
                        consumer.commitSync()
                        ack
                      } catch {
                        case NonFatal(ex) =>
                          onError(ex)
                          Stop
                      }
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
