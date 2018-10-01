/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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

import java.nio.ByteBuffer
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import org.apache.kafka.common.utils.Bytes

/** Wraps a Kafka `Serializer`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  *
  * @param className is the full package path to the Kafka `Serializer`
  *
  * @param classType is the `java.lang.Class` for [[className]]
  *
  * @param constructor creates an instance of [[classType]].
  *        This is defaulted with a `Serializer.Constructor[A]` function that creates a
  *        new instance using an assumed empty constructor.
  *        Supplying this parameter allows for manual provision of the `Serializer`.
  */
final case class Serializer[A](
  className: String,
  classType: Class[_ <: KafkaSerializer[A]],
  constructor: Serializer.Constructor[A] = (s: Serializer[A]) => s.classType.newInstance()) {

  /** Creates a new instance. */
  def create(): KafkaSerializer[A] =
    constructor(this)
}

object Serializer {

  /** Alias for the function that provides an instance of
    * the Kafka `Serializer`.
    */
  type Constructor[A] = (Serializer[A]) => KafkaSerializer[A]

  implicit val forStrings: Serializer[String] =
    Serializer[String](
      className = "org.apache.kafka.common.serialization.StringSerializer",
      classType = classOf[StringSerializer]
    )

  implicit val forByteArray: Serializer[Array[Byte]] =
    Serializer[Array[Byte]](
      className = "org.apache.kafka.common.serialization.ByteArraySerializer",
      classType = classOf[ByteArraySerializer]
    )

  implicit val forByteBuffer: Serializer[ByteBuffer] =
    Serializer[ByteBuffer](
      className = "org.apache.kafka.common.serialization.ByteBufferSerializer",
      classType = classOf[ByteBufferSerializer]
    )

  implicit val forBytes: Serializer[Bytes] =
    Serializer[Bytes](
      className = "org.apache.kafka.common.serialization.BytesSerializer",
      classType = classOf[BytesSerializer]
    )

  implicit val forJavaDouble: Serializer[java.lang.Double] =
    Serializer[java.lang.Double](
      className = "org.apache.kafka.common.serialization.DoubleSerializer",
      classType = classOf[DoubleSerializer]
    )

  implicit val forJavaInteger: Serializer[java.lang.Integer] =
    Serializer[java.lang.Integer](
      className = "org.apache.kafka.common.serialization.IntegerSerializer",
      classType = classOf[IntegerSerializer]
    )

  implicit val forJavaLong: Serializer[java.lang.Long] =
    Serializer[java.lang.Long](
      className = "org.apache.kafka.common.serialization.LongSerializer",
      classType = classOf[LongSerializer]
    )
}
