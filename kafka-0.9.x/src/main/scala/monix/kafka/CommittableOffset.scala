/*
 * Copyright (c) 2014-2019 by The Monix Project Developers.
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

import monix.eval.Task
import org.apache.kafka.common.TopicPartition

/** Represents offset for specified topic and partition that can be
  * committed by [[commit]] method call.
  * To achieve good performance it is recommended to use batched commit with
  * [[CommittableOffsetBatch]] class.
  *
  * @param topicPartition is the topic and partition identifier
  *
  * @param offset is the offset to be committed
  *
  * @param commitBatch is the function for batched commit realized as closure
  *        in [[KafkaConsumerObservable]] context. This decision was made for
  *        thread-safety reasons.
  */
final class CommittableOffset private[kafka] (
  val topicPartition: TopicPartition,
  val offset: Long,
  private[kafka] val commitBatch: Map[TopicPartition, Long] => Task[Unit]) {

  /**
    * Commits [[offset]] for the [[topicPartition]] to Kafka. It is recommended
    * to use batched commit with [[CommittableOffsetBatch]] class.
    * */
  def commit(): Task[Unit] = commitBatch(Map(topicPartition -> offset))
}

object CommittableOffset {

  def apply(
    topicPartition: TopicPartition,
    offset: Long,
    commitBatch: Map[TopicPartition, Long] => Task[Unit]): CommittableOffset =
    new CommittableOffset(topicPartition, offset, commitBatch)
}
