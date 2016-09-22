package monix.kafka

import java.nio.ByteBuffer
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import org.apache.kafka.common.utils.Bytes
import language.existentials

/** Wraps a Kafka `Serializer`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  */
final case class Serializer[A](
  className: String,
  classType: Class[_ <: KafkaSerializer[A]]) {

  /** Creates a new instance. */
  def create(): KafkaSerializer[A] =
    classType.newInstance()
}

object Serializer {
  implicit val forStrings: Serializer[String] =
    Serializer(
      className = "org.apache.kafka.common.serialization.StringSerializer",
      classType = classOf[StringSerializer]
    )

  implicit val forByteArray: Serializer[Array[Byte]] =
    Serializer(
      className = "org.apache.kafka.common.serialization.ByteArraySerializer",
      classType = classOf[ByteArraySerializer]
    )

  implicit val forByteBuffer: Serializer[ByteBuffer] =
    Serializer(
      className = "org.apache.kafka.common.serialization.ByteBufferSerializer",
      classType = classOf[ByteBufferSerializer]
    )

  implicit val forBytes: Serializer[Bytes] =
    Serializer(
      className = "org.apache.kafka.common.serialization.BytesSerializer",
      classType = classOf[BytesSerializer]
    )

  implicit val forJavaDouble: Serializer[java.lang.Double] =
    Serializer(
      className = "org.apache.kafka.common.serialization.DoubleSerializer",
      classType = classOf[DoubleSerializer]
    )

  implicit val forJavaInteger: Serializer[java.lang.Integer] =
    Serializer(
      className = "org.apache.kafka.common.serialization.IntegerSerializer",
      classType = classOf[IntegerSerializer]
    )

  implicit val forJavaLong: Serializer[java.lang.Long] =
    Serializer(
      className = "org.apache.kafka.common.serialization.LongSerializer",
      classType = classOf[LongSerializer]
    )
}
