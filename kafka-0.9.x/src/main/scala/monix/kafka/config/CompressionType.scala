/*
 * Copyright (c) 2014-2022 by The Monix Project Developers.
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

/** The compression type for all data generated by the producer,
  * the `compression.type` from the Kafka Producer configuration.
  *
  * The default is none (i.e. no compression). Compression is of full
  * batches of data, so the efficacy of batching will also impact
  * the compression ratio (more batching means better compression).
  *
  * Valid values:
  *
  *  - [[CompressionType.Uncompressed]]
  *  - [[CompressionType.Gzip]]
  *  - [[CompressionType.Snappy]]
  *  - [[CompressionType.Lz4]]
  */
sealed trait CompressionType extends Serializable {
  def id: String
}

object CompressionType {

  @throws(classOf[BadValue])
  def apply(id: String): CompressionType =
    id match {
      case Uncompressed.id => Uncompressed
      case Gzip.id => Gzip
      case Snappy.id => Snappy
      case Lz4.id => Lz4
      case _ =>
        throw new BadValue("kafka.compression.type", s"Invalid value: $id")
    }

  case object Uncompressed extends CompressionType {
    val id = "none"
  }

  case object Gzip extends CompressionType {
    val id = "gzip"
  }

  case object Snappy extends CompressionType {
    val id = "snappy"
  }

  case object Lz4 extends CompressionType {
    val id = "lz4"
  }
}
