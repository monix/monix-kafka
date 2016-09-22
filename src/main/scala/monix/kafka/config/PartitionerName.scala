package monix.kafka.config

import org.apache.kafka.clients.producer.Partitioner
import scala.reflect.ClassTag

final case class PartitionerName(className: String)
  extends ClassName[Partitioner] {

  /** Creates a new instance of the referenced `Serializer`. */
  def createInstance(): Partitioner =
    classType.newInstance()
}

object PartitionerName {
  /** Builds a [[SerializerName]], given a class. */
  def apply[C <: Partitioner](implicit C: ClassTag[C]): PartitionerName =
    PartitionerName(C.runtimeClass.getCanonicalName)

  /** Returns the default `Partitioner` instance. */
  val default: PartitionerName =
    PartitionerName("org.apache.kafka.clients.producer.internals.DefaultPartitioner")
}

