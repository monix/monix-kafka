package monix.kafka

import java.nio.ByteBuffer
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization.{Deserializer => KafkaDeserializer}
import org.apache.kafka.common.utils.Bytes
import language.existentials

/** Wraps a Kafka `Deserializer`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  */
final case class Deserializer[A](
  className: String,
  classType: Class[_ <: KafkaDeserializer[A]]) {

  /** Creates a new instance. */
  def create(): KafkaDeserializer[A] =
  classType.newInstance()
}

object Deserializer {
  implicit val forStrings: Deserializer[String] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.StringDeserializer",
      classType = classOf[StringDeserializer]
    )

  implicit val forByteArray: Deserializer[Array[Byte]] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      classType = classOf[ByteArrayDeserializer]
    )

  implicit val forByteBuffer: Deserializer[ByteBuffer] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.ByteBufferDeserializer",
      classType = classOf[ByteBufferDeserializer]
    )

  implicit val forBytes: Deserializer[Bytes] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.BytesDeserializer",
      classType = classOf[BytesDeserializer]
    )

  implicit val forJavaDouble: Deserializer[java.lang.Double] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.DoubleDeserializer",
      classType = classOf[DoubleDeserializer]
    )

  implicit val forJavaInteger: Deserializer[java.lang.Integer] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.IntegerDeserializer",
      classType = classOf[IntegerDeserializer]
    )

  implicit val forJavaLong: Deserializer[java.lang.Long] =
    Deserializer(
      className = "org.apache.kafka.common.serialization.LongDeserializer",
      classType = classOf[LongDeserializer]
    )
}
