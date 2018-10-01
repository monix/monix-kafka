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

import scala.reflect.ClassTag

abstract class ClassName[T](implicit T: ClassTag[T]) extends Serializable {
  def className: String

  val classType: Class[_ <: T] =
    Class.forName(className).asInstanceOf[Class[_ <: T]]

  require(
    findClass(classType :: Nil, T.runtimeClass),
    s"Given type $className does not implement ${T.runtimeClass}"
  )

  private def findClass(stack: List[Class[_]], searched: Class[_]): Boolean =
    stack match {
      case Nil => false
      case x :: xs =>
        if (x == searched) true else {
          val superClass: List[Class[_]] = Option(x.getSuperclass).toList
          val rest = superClass ::: x.getInterfaces.toList ::: xs
          findClass(rest, searched)
        }
    }
}
