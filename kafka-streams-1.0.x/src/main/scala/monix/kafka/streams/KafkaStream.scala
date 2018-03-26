package monix.kafka.streams

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KStream, Printed, Produced}

import scala.collection.JavaConverters._
import scala.language.postfixOps

/** Scala wrapper for [[org.apache.kafka.streams.kstream.KStream]]
  *
  * @param underlyingStream the underlying [[org.apache.kafka.streams.kstream.KStream]] object
  * @tparam K key type
  * @tparam V value type
  */
final case class KafkaStream[K, V] private[streams](underlyingStream: KStream[K, V]) {

  def map[K1, V1](f: (K, V) => (K1, V1)): KafkaStream[K1, V1] =
    copy(
      underlyingStream = underlyingStream.map[K1, V1] { (key, value) =>
        val (newKey, newValue) = f(key, value)
        new KeyValue[K1, V1](newKey, newValue)
      }
    )

  def mapValues[V1](f: V => V1): KafkaStream[K, V1] =
    copy(underlyingStream = underlyingStream.mapValues[V1](f(_)))

  def flatMap[K1, V1](f: (K, V) => Iterable[(K1, V1)]): KafkaStream[K1, V1] =
    copy(
      underlyingStream = underlyingStream.flatMap[K1, V1] { (key: K, value: V) =>
        f(key, value).map { case (k, v) => new KeyValue[K1, V1](k, v) } asJava
      }
    )

  def to(topic: String): Unit =
    underlyingStream.to(topic)

  def producedTo(topic: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): KafkaStream[K, V] = {
    underlyingStream.to(topic, Produced.`with`(keySerde, valueSerde))
    this
  }

  def toSysOut(label: String): KafkaStream[K, V] = {
    underlyingStream.print(Printed.toSysOut[K, V].withLabel(label))
    this
  }

}

object KafkaStream {

  implicit val stringSerde: Serde[String] = Serdes.String
}
