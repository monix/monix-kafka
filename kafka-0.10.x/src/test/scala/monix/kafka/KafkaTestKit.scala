package monix.kafka

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterEach, Suite}

trait KafkaTestKit extends BeforeAndAfterEach { self: Suite =>

  sys.addShutdownHook {
    EmbeddedKafka.stop()
  }

  override def beforeEach(): Unit = {
    EmbeddedKafka.start()
  }

  override def afterEach(): Unit = {
    EmbeddedKafka.stop()
  }

}
