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
