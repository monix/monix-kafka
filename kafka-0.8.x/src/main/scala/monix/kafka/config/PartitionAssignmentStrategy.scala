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

package monix.kafka.config

import com.typesafe.config.ConfigException.BadValue

/** A strategy for assigning partitions to consumer streams
  *
  * Available choices:
  *
  *  - [[PartitionAssignmentStrategy.Range]]
  *  - [[PartitionAssignmentStrategy.Roundrobin]]
  */
sealed trait PartitionAssignmentStrategy extends Serializable {
  def id: String
}

object PartitionAssignmentStrategy {
  @throws(classOf[BadValue])
  def apply(id: String): PartitionAssignmentStrategy =
    id.trim.toLowerCase match {
      case Range.id => Range
      case Roundrobin.id => Roundrobin
      case _ =>
        throw new BadValue("kafka.partition.assignment.strategy", s"Invalid value: $id")
    }

  /** Use range method for assigning partitions. */
  case object Range extends PartitionAssignmentStrategy {
    val id = "range"
  }

  /** Use roundrobin method for assigning partitions. */
  case object Roundrobin extends PartitionAssignmentStrategy {
    val id = "roundrobin"
  }

}



