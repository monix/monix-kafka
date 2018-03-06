package monix.kafka

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, Suite}

trait KafkaTestKit extends BeforeAndAfterAll { self: Suite =>

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    sys.addShutdownHook {
      EmbeddedKafka.stop()
    }
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

}
