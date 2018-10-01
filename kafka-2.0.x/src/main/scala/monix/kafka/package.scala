/*
 * Copyright (c) 2014-2018 by its authors. Some rights reserved.
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
package monix

import java.time.temporal.ChronoUnit
import java.time.{Duration => JavaDuration}
import java.util.concurrent.TimeUnit

package object kafka {

  // TODO alternatively depend on scala-java8-compat package
  implicit class DurationConverter(duration: scala.concurrent.duration.FiniteDuration) {
    /**
      * Transform a Scala FiniteDuration into a Java duration. Note that the Scala duration keeps the time unit it was created
      * with while a Java duration always is a pair of seconds and nanos, so the unit it lost.
      */
    final def toJava: JavaDuration = {
      if (duration.length == 0) JavaDuration.ZERO
      else duration.unit match {
        case TimeUnit.NANOSECONDS => JavaDuration.ofNanos(duration.length)
        case TimeUnit.MICROSECONDS => JavaDuration.of(duration.length, ChronoUnit.MICROS)
        case TimeUnit.MILLISECONDS => JavaDuration.ofMillis(duration.length)
        case TimeUnit.SECONDS => JavaDuration.ofSeconds(duration.length)
        case TimeUnit.MINUTES => JavaDuration.ofMinutes(duration.length)
        case TimeUnit.HOURS => JavaDuration.ofHours(duration.length)
        case TimeUnit.DAYS => JavaDuration.ofDays(duration.length)
      }
    }
  }

}
