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

/** Batch of Kafka offsets which can be committed together.
  * Can be builded from offsets sequence by [[CommittableOffsetBatch#apply]] method.
  * Besides you can use [[CommittableOffsetBatch#empty]] method to create empty batch and
  * add offsets to it by [[updated]] method.
  *
  * Anyway offsets order make sense! Only last added offset for topic and partition will
  * be committed to Kafka.
  *
  * @param offsets is the offsets batch for a few topics and partitions.
  *        Make sure that each of them was received from one [[KafkaConsumerObservable]].
  *
  * @param commitBatch is the function for batched commit realized as closure
  *        in [[KafkaConsumerObservable]] context. This decision was made for
  *        thread-safety reasons. This parameter is obtained from last [[CommittableOffset]]
  *        added to batch.
  */
final class CommittableOffsetBatch(
  val offsets: Map[TopicPartition, Long],
  commitBatch: Map[TopicPartition, Long] => Task[Unit]) {

  /**
    * Commits [[offsets]] to Kafka
    * */
  def commit(): Task[Unit] = commitBatch(offsets)

  /**
    * Adds new [[CommittableOffset]] to batch. Added offset replaces previous one specified
    * for same topic and partition.
    * */
  def updated(committableOffset: CommittableOffset): CommittableOffsetBatch =
    new CommittableOffsetBatch(
      offsets.updated(committableOffset.topicPartition, committableOffset.offset),
      committableOffset.commitBatch
    )
}

object CommittableOffsetBatch {

  /**
    * Creates empty [[CommittableOffsetBatch]]. Can be used as neutral element in fold:
    * {{{
    *   offsets.foldLeft(CommittableOffsetBatch.empty)(_ updated _)
    * }}}
    * */
  def empty: CommittableOffsetBatch = new CommittableOffsetBatch(Map.empty, _ => Task.pure(()))

  /**
    * Builds [[CommittableOffsetBatch]] from offsets sequence. Be careful with
    * sequence order. If there is more than once offset for a topic and partition in the
    * sequence then the last one will remain.
    * */
  def apply(offsets: Seq[CommittableOffset]): CommittableOffsetBatch =
    if (offsets.nonEmpty) {
      val aggregatedOffsets = offsets.foldLeft(Map.empty[TopicPartition, Long]) { (acc, o) =>
        acc.updated(o.topicPartition, o.offset)
      }
      new CommittableOffsetBatch(aggregatedOffsets, offsets.head.commitBatch)
    } else {
      empty
    }
}
