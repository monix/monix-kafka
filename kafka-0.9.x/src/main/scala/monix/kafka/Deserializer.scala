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

import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization.{Deserializer => KafkaDeserializer}
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
