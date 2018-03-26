package monix.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

/**
  * Class for managing the stream builder and topology
  */
final class StreamTopology private {

  private val builder: StreamsBuilder = new StreamsBuilder

  def stream[K, V](topic: String): KafkaStream[K, V] =
    KafkaStream(builder.stream[K, V](topic))

  def streamWith[K, V](topic: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): KafkaStream[K, V] =
    KafkaStream(builder.stream[K, V](topic, Consumed.`with`(keySerde, valueSerde)))

  private[streams] def build: Topology =
    builder.build()
}

object StreamTopology {

  def apply(): StreamTopology =
    new StreamTopology

}
