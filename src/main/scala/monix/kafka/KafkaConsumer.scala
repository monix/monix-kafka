package monix.kafka

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Ack.{Continue, Stop}
import monix.execution.FutureUtils.extensions._
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, Observer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer => ApacheKafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** An `Observable` implementation that reads messages from `Kafka`.
  */
final class KafkaConsumer[K, V]
  (kafkaConsumerTask: Task[ApacheKafkaConsumer[K,V]], config: KafkaConsumerConfig, ioScheduler: Scheduler)
  (implicit K: Deserializer[K], V: Deserializer[V])
  extends Observable[ConsumerRecord[K, V]] with StrictLogging {

  override def unsafeSubscribeFn(subscriber: Subscriber[ConsumerRecord[K, V]]): Cancelable =
    kafkaConsumerTask
      .map(kafkaConsumer => underlying(kafkaConsumer).unsafeSubscribeFn(subscriber))
      .runAsync(ioScheduler)


  private def currentSubscriptions(consumerRef: ApacheKafkaConsumer[K,V]) =
    consumerRef.subscription().asScala.toSet

  private def loop(consumer: ApacheKafkaConsumer[K,V], out: Subscriber[ConsumerRecord[K,V]]): Task[Unit] = {
    val consumeTask = Task.unsafeCreate[Ack] { (s, conn, cb) =>
      // Forced asynchronous boundary, in order to not block
      // the current thread!
      ioScheduler.executeAsync {

        implicit val scheduler = ioScheduler

        val pollRecordsF =
          if (currentSubscriptions(consumer).nonEmpty)
            Future.fromTry(Try(consumer.poll(config.fetchMaxWaitTime.toMillis)))
          else
            Future.successful(ConsumerRecords.empty[K, V]())

        val ack = pollRecordsF.flatMap { pollRecords =>
          Observer
            .feed(out, pollRecords.asScala)
            .materialize
            .flatMap {
              case Success(ackSuccess) =>
                if (!config.enableAutoCommit && currentSubscriptions(consumer).nonEmpty)
                  Future.fromTry(
                    Try { consumer.commitSync(); ackSuccess }
                      .recoverWith { case ex => Try { consumer.close(); Stop } }
                  )
                else
                  ackSuccess
              case Failure(ex) =>
                Future.fromTry(Try {consumer.close(); Stop })
            }
        }

        ack.syncOnComplete {
          case Success(value) =>
            cb.asyncOnSuccess(value)(s)
          case Failure(ex) =>
            cb.asyncOnError(ex)(s)
        }
      }
    }

    consumeTask.flatMap {
      case Stop => Task.unit
      case Continue => loop(consumer, out)
    }
  }

  private def underlying(kafkaConsumer: ApacheKafkaConsumer[K, V]): Observable[ConsumerRecord[K, V]] =
    Observable.unsafeCreate[ConsumerRecord[K, V]] { subscriber =>
      loop(kafkaConsumer, subscriber).runAsync(ioScheduler)
    }
}

object KafkaConsumer {
  def apply[K, V](config: KafkaConsumerConfig, ioScheduler: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumer[K,V] =
    apply(new ApacheKafkaConsumer[K,V](config.toProperties, K.create(), V.create()), config, ioScheduler)

  def apply[K, V](kafkaConsumer: ApacheKafkaConsumer[K,V], config: KafkaConsumerConfig, ioScheduler: Scheduler)
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumer[K,V] =
    new KafkaConsumer(Task.eval(kafkaConsumer), config, ioScheduler)
}
