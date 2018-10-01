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

/** Specifies the consumer commit order, to use by the
  * [[monix.kafka.KafkaConsumerObservable KafkaConsumerObservable]]
  * in case `kafka.enable.auto.commit` is set to `false`.
  *
  * Available options:
  *
  *  - [[ObservableCommitOrder.BeforeAck]] specifies to do a commit
  *    before acknowledgement is received from downstream
  *  - [[ObservableCommitOrder.AfterAck]] specifies to do a commit
  *    after acknowledgement is received from downstream
  *  - [[ObservableCommitOrder.NoAck]] specifies to skip committing
  */
sealed trait  ObservableCommitOrder extends Serializable {
  def id: String

  def isBefore: Boolean =
    this match {
      case ObservableCommitOrder.BeforeAck => true
      case _ => false
    }

  def isAfter: Boolean =
    this match {
      case ObservableCommitOrder.AfterAck => true
      case _ => false
    }
}

object ObservableCommitOrder {
  @throws(classOf[BadValue])
  def apply(id: String): ObservableCommitOrder =
    id match {
      case BeforeAck.id => BeforeAck
      case AfterAck.id => AfterAck
      case NoAck.id => NoAck
      case _ =>
        throw new BadValue(
          "kafka.monix.observable.commit.order",
          s"Invalid value: $id")
    }

  /** Do a `commit` in the Kafka Consumer before
    * receiving an acknowledgement from downstream.
    */
  case object BeforeAck extends ObservableCommitOrder {
    val id = "before-ack"
  }

  /** Do a `commit` in the Kafka Consumer after
    * receiving an acknowledgement from downstream.
    */
  case object AfterAck extends ObservableCommitOrder {
    val id = "after-ack"
  }

  /** Do not `commit` in the Kafka Consumer.
    */
  case object NoAck extends ObservableCommitOrder {
    val id = "no-ack"
  }
}
