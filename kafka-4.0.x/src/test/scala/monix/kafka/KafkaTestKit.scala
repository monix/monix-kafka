package monix.kafka

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.Suite

trait KafkaTestKit extends EmbeddedKafka { self: Suite =>

  sys.addShutdownHook {
    EmbeddedKafka.stop()
  }
}
