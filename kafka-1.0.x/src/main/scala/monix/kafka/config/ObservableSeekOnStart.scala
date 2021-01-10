/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
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

/** Specifies whether to call `seekToEnd` or `seekToBeginning` when starting
  * [[monix.kafka.KafkaConsumerObservable KafkaConsumerObservable]]
  *
  * Available options:
  *
  *  - [[ObservableSeekOnStart.End]]
  *  - [[ObservableSeekOnStart.Beginning]]
  *  - [[ObservableSeekOnStart.NoSeek]]
  */
sealed trait ObservableSeekOnStart extends Serializable {
  def id: String

  def isSeekBeginning: Boolean =
    this match {
      case ObservableSeekOnStart.Beginning => true
      case _ => false
    }

  def isSeekEnd: Boolean =
    this match {
      case ObservableSeekOnStart.End => true
      case _ => false
    }
}

object ObservableSeekOnStart {

  @throws(classOf[BadValue])
  def apply(id: String): ObservableSeekOnStart =
    id match {
      case End.id => End
      case Beginning.id => Beginning
      case NoSeek.id => NoSeek
      case _ =>
        throw new BadValue("kafka.monix.observable.seek.onStart", s"Invalid value: $id")
    }

  /** Calls `consumer.seekToEnd()` when starting consumer.
    */
  case object End extends ObservableSeekOnStart {
    val id = "end"
  }

  /** Calls `consumer.seekToBeginning()` when starting consumer.
    */
  case object Beginning extends ObservableSeekOnStart {
    val id = "beginning"
  }

  /** Does not call neither `consumer.seekToEnd()` nor `consumer.seekToBeginning`
    * when starting consumer.
    */
  case object NoSeek extends ObservableSeekOnStart {
    val id = "no-seek"
  }
}
