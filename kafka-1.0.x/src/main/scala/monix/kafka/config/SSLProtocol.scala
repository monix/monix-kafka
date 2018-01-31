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

import java.security.NoSuchAlgorithmException
import javax.net.ssl.SSLContext

import com.typesafe.config.ConfigException.BadValue

/** Represents the available protocols to use for
  * SSL connections.
  *
  * Available values:
  *
  *  - [[SSLProtocol.TLSv12]]
  *  - [[SSLProtocol.TLSv11]]
  *  - [[SSLProtocol.TLSv1]]
  *  - [[SSLProtocol.TLS]]
  *  - [[SSLProtocol.SSLv3]] (prefer only for older JVMs)
  *  - [[SSLProtocol.SSLv2]] (prefer only for older JVMs, no longer available for Java 8)
  *  - [[SSLProtocol.SSL]] (prefer only for older JVMs)
  */
sealed trait SSLProtocol extends Serializable {
  def id: String

  def getInstance(): Option[SSLContext] =
    try Some(SSLContext.getInstance(id)) catch {
      case _: NoSuchAlgorithmException =>
        None
    }
}

object SSLProtocol {
  @throws(classOf[BadValue])
  def apply(id: String): SSLProtocol = {
    val algorithm = id match {
      case TLSv12.id => TLSv12
      case TLSv11.id => TLSv11
      case TLSv1.id => TLSv1
      case TLS.id => TLS
      case SSLv3.id => SSLv3
      case SSLv2.id => SSLv2
      case SSL.id => SSL
      case _ =>
        throw new BadValue("kafka.ssl.enabled.protocols", s"Invalid value: $id")
    }

    algorithm.getInstance() match {
      case Some(_) => algorithm
      case None =>
        throw new BadValue("kafka.ssl.enabled.protocols", s"Unsupported SSL protocol: $id")
    }
  }

  case object TLSv12 extends SSLProtocol {
    val id = "TLSv1.2"
  }

  case object TLSv11 extends SSLProtocol {
    val id = "TLSv1.1"
  }

  case object TLSv1 extends SSLProtocol {
    val id = "TLSv1"
  }

  case object TLS extends SSLProtocol {
    val id = "TLS"
  }

  /** WARNING: deprecated, might not work on recent versions
    * of the JVM. Prefer TLS.
    */
  case object SSLv3 extends SSLProtocol {
    val id = "SSLv3"
  }

  /** WARNING: deprecated, might not work on recent versions
    * of the JVM. Prefer TLS.
    */
  case object SSLv2 extends SSLProtocol {
    val id = "SSLv2"
  }

  /** WARNING: deprecated, might not work on recent versions
    * of the JVM. Prefer TLS.
    */
  case object SSL extends SSLProtocol {
    val id = "SSL"
  }
}
