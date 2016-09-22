package monix.kafka

import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Callback, Coeval, Task}
import monix.execution.Ack.Continue
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** A `monix.reactive.Consumer` that pushes incoming messages into
  * a [[KafkaProducer]].
  */
final class KafkaProducerSink[K,V] private (
  producer: Coeval[KafkaProducer[K,V]],
  shouldTerminate: Boolean)
  extends Consumer[ProducerRecord[K,V], Unit]
  with StrictLogging with Serializable {

  def createSubscriber(cb: Callback[Unit], s: Scheduler) = {
    val out = new Subscriber[ProducerRecord[K,V]] {
      implicit val scheduler = s
      private[this] val p = producer.memoize

      def onNext(elem: ProducerRecord[K, V]): Future[Ack] = {
        val task = try p.value.send(elem)
        catch { case NonFatal(ex) => Task.raiseError(ex) }

        val recovered = task.map(_ => Continue).onErrorHandle { ex =>
          logger.error("Unexpected error in KafkaProducerSink", ex)
          Continue
        }

        recovered.runAsync
      }

      def terminate(cb: => Unit): Unit = {
        if (!shouldTerminate) cb else
          Task(p.value.close()).flatten.materialize.runAsync.foreach {
            case Success(_) => cb
            case Failure(ex) =>
              logger.error("Unexpected error in KafkaProducerSink", ex)
              cb
          }
      }

      def onError(ex: Throwable): Unit =
        terminate(cb.onError(ex))
      def onComplete(): Unit =
        terminate(cb.onSuccess(()))
    }

    (out, AssignableCancelable.dummy)
  }
}

object KafkaProducerSink {
  /** Builder for [[KafkaProducerSink]]. */
  def apply[K,V](config: KafkaProducerConfig, io: Scheduler)
    (implicit K: Serializer[K], V: Serializer[V]): KafkaProducerSink[K,V] = {

    val producer = Coeval(KafkaProducer[K,V](config, io))
    new KafkaProducerSink(producer, shouldTerminate = true)
  }

  /** Builder for [[KafkaProducerSink]]. */
  def apply[K,V](producer: Coeval[KafkaProducer[K,V]]): KafkaProducerSink[K,V] =
    new KafkaProducerSink(producer, shouldTerminate = false)
}
