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

import com.typesafe.config.ConfigException.BadValue

/** What to do when there is no initial offset in Kafka or if the
  * current offset does not exist any more on the server
  * (e.g. because that data has been deleted).
  *
  * Available choices:
  *
  *  - [[AutoOffsetReset.Earliest]]
  *  - [[AutoOffsetReset.Latest]]
  *  - [[AutoOffsetReset.Throw]]
  */
sealed trait AutoOffsetReset extends Serializable {
  def id: String
}

object AutoOffsetReset {
  @throws(classOf[BadValue])
  def apply(id: String): AutoOffsetReset =
    id.trim.toLowerCase match {
      case Earliest.id => Earliest
      case Latest.id => Latest
      case Throw.id => Throw
      case _ =>
        throw new BadValue("kafka.auto.offset.reset", s"Invalid value: $id")
    }

  /** Automatically reset the offset to the earliest offset. */
  case object Earliest extends AutoOffsetReset {
    val id = "earliest"
  }

  /** Automatically reset the offset to the latest offset. */
  case object Latest extends AutoOffsetReset {
    val id = "latest"
  }

  /** Throw exception to the consumer if no previous offset
    * is found for the consumer's group.
    */
  case object Throw extends AutoOffsetReset {
    val id = "none"
  }
}