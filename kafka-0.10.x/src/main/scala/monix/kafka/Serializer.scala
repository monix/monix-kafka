/*
 * Copyright (c) 2014-2016 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monixio/monix-kafka
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
import language.existentials

/** Wraps a Kafka `Serializer`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  */
final case class Serializer[A](
  className: String,
  classType: Class[_ <: KafkaSerializer[A]],
  classInstance: Serializer.ClassInstanceProvider[A] = (s: Serializer[A]) => s.classType.newInstance()) {

  /** Creates a new instance. */
  def create(): KafkaSerializer[A] =
    classInstance(this)
}

object Serializer {

  type ClassInstanceProvider[A] = (Serializer[A]) => KafkaSerializer[A]

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
