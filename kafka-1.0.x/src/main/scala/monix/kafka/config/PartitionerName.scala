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

package monix.kafka.config

import org.apache.kafka.clients.producer.Partitioner
import scala.reflect.ClassTag

final case class PartitionerName(className: String)
  extends ClassName[Partitioner] {

  /** Creates a new instance of the referenced `Serializer`. */
  def createInstance(): Partitioner =
    classType.newInstance()
}

object PartitionerName {
  /** Builds a [[PartitionerName]], given a class. */
  def apply[C <: Partitioner](implicit C: ClassTag[C]): PartitionerName =
    PartitionerName(C.runtimeClass.getCanonicalName)

  /** Returns the default `Partitioner` instance. */
  val default: PartitionerName =
    PartitionerName("org.apache.kafka.clients.producer.internals.DefaultPartitioner")
}

