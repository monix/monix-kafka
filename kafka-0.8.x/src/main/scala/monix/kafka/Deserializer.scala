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

import kafka.serializer.{DefaultDecoder, StringDecoder, Decoder => KafkaDecoder}
import language.existentials

/** Wraps a Kafka `Decoder`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  */
final case class Deserializer[A](
  className: String,
  classType: Class[_ <: KafkaDecoder[A]]) {

  /** Creates a new instance. */
  def create(): KafkaDecoder[A] = {
    val constructor = classType.getDeclaredConstructors()(0)
    constructor.getParameterCount match {
      case 0 => classType.newInstance()
      case 1 => constructor.newInstance(null).asInstanceOf[KafkaDecoder[A]]
    }
  }
}

object Deserializer {
  implicit val forStrings: Deserializer[String] =
    Deserializer(
      className = "kafka.serializer.StringDecoder",
      classType = classOf[StringDecoder]
    )

  implicit val forByteArray: Deserializer[Array[Byte]] =
    Deserializer(
      className = "kafka.serializer.DefaultDecoder",
      classType = classOf[DefaultDecoder]
    )
}
