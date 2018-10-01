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

/** Specifies the consumer commit type, to use by the
  * [[monix.kafka.KafkaConsumerObservable KafkaConsumerObservable]]
  * in case `kafka.enable.auto.commit` is set to `false`.
  *
  * Available options:
  *
  *  - [[ObservableCommitType.Sync]]
  *  - [[ObservableCommitType.Async]]
  */
sealed trait ObservableCommitType extends Serializable {
  def id: String
}

object ObservableCommitType {
  @throws(classOf[BadValue])
  def apply(id: String): ObservableCommitType =
    id match {
      case Sync.id => Sync
      case Async.id => Async
      case _ =>
        throw new BadValue(
          "kafka.monix.observable.commit.type",
          s"Invalid value: $id")
    }

  /** Uses `consumer.commitSync()` after each batch
    * if `enable.auto.commit` is `false`.
    */
  case object Sync extends ObservableCommitType {
    val id = "sync"
  }

  /** Uses `consumer.commitAsync()` after each batch
    * if `enable.auto.commit` is `false`.
    */
  case object Async extends ObservableCommitType {
    val id = "async"
  }
}
