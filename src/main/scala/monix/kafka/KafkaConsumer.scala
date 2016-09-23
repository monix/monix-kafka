package monix.kafka

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Ack.Stop
import monix.execution.FutureUtils.extensions.FutureExtensions
import monix.execution.cancelables.SingleAssignmentCancelable
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer => ApacheKafkaConsumer}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.collection.JavaConversions.{asScalaIterator, seqAsJavaList}

/** An `Observable` implementation that reads messages from `Kafka`. */
final class KafkaConsumer private
(config: KafkaConsumerConfig, topic: String, ioScheduler: Scheduler)
  extends Observable[(String, String)] with StrictLogging {

  def unsafeSubscribeFn(subscriber: Subscriber[(String, String)]): Cancelable = {
    val source = underlying.onErrorHandleWith { ex =>
      logger.error(
        s"Unexpected error in KafkaConsumerObservable, retrying in ${config.retryBackoffTime}", ex)
      // Applying configured delay before retry!
      underlying.delaySubscription(config.retryBackoffTime)
    }

    source.unsafeSubscribeFn(subscriber)
  }

  private val underlying =
    Observable.unsafeCreate[(String, String)] { subscriber =>
      val cancelable = SingleAssignmentCancelable()

      // Forced asynchronous boundary, in order to not block
      // the current thread!
      ioScheduler.executeAsync {
        val configProps = config.toProperties

        logger.info(s"Kafka Consumer connecting with $configProps")

        val stringDeserializer = new StringDeserializer()
        val consumer =
          new ApacheKafkaConsumer[String, String](configProps, stringDeserializer, stringDeserializer)

        consumer.subscribe(List(topic))
        logger.info(s"Subscribing to topic : $topic")

        cancelable := Observable
          .fromIterator(consumer.poll(config.fetchMaxWaitTime.toMillis).iterator(), () => consumer.close())
          .unsafeSubscribeFn(new Subscriber[ConsumerRecord[String, String]] {
            self =>

            implicit val scheduler = ioScheduler
            private[this] var isDone = false

            def onNext(elem: ConsumerRecord[String, String]): Future[Ack] =
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
  def apply(config: KafkaConsumerConfig, ioScheduler: Scheduler)(topic: String): KafkaConsumer =
    new KafkaConsumer(config, topic, ioScheduler)
}
