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
  *
  * @param className is the full package path to the Kafka `Decoder`
  *
  * @param classType is the `java.lang.Class` for [[className]]
  *
  * @param constructor creates an instance of [[classType]].
  *        This is defaulted with a `Deserializer.Constructor[A]` function that creates a
  *        new instance using an assumed empty or nullable constructor.
  *        Supplying this parameter allows for manual provision of the `Decoder`.
  */
final case class Deserializer[A](
  className: String,
  classType: Class[_ <: KafkaDecoder[A]],
  constructor: Deserializer.Constructor[A] = Deserializer.reflectCreate[A] _) {

  /** Creates a new instance. */
  def create(): KafkaDecoder[A] =
    constructor(this)
}

object Deserializer {

  /** Alias for the function that provides an instance of
    * the Kafka `Decoder`.
    */
  type Constructor[A] = (Deserializer[A]) => KafkaDecoder[A]

  private def reflectCreate[A](d: Deserializer[A]): KafkaDecoder[A] = {
    val constructor = d.classType.getDeclaredConstructors()(0)
    constructor.getParameterCount match {
      case 0 => d.classType.newInstance()
      case 1 => constructor.newInstance(null).asInstanceOf[KafkaDecoder[A]]
    }
  }

  implicit val forStrings: Deserializer[String] =
    Deserializer[String](
      className = "kafka.serializer.StringDecoder",
      classType = classOf[StringDecoder]
    )

  implicit val forByteArray: Deserializer[Array[Byte]] =
    Deserializer[Array[Byte]](
      className = "kafka.serializer.DefaultDecoder",
      classType = classOf[DefaultDecoder]
    )
}
