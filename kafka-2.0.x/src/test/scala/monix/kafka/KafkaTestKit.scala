package monix.kafka

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, Suite}

trait KafkaTestKit extends BeforeAndAfterAll { self: Suite =>

  sys.addShutdownHook {
    EmbeddedKafka.stop()
  }

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

}