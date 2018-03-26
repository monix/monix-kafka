package monix.kafka.streams

import monix.eval.Task
import org.apache.kafka.streams.{StreamsConfig, KafkaStreams => ApacheKafkaStreams}

/** The wrapper for [[org.apache.kafka.streams.KafkaStreams]]
  *
  * @param underlying the underlying [[org.apache.kafka.streams.KafkaStreams]] object
  */
final class KafkaStreams private[streams](underlying: ApacheKafkaStreams) {

  def start: Task[Unit] =
    Task.eval(underlying.start())

  def close: Task[Unit] =
    Task.eval(underlying.close())

}

object KafkaStreams {

  def apply[K, V](config: KafkaStreamsConfig, topology: StreamTopology) = {
    new KafkaStreams(
      new ApacheKafkaStreams(topology.build, new StreamsConfig(config.toProperties))
    )
  }
}
