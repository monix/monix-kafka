package monix.kafka

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait KafkaTestKit extends BeforeAndAfterAll { self: Suite =>

  sys.addShutdownHook {
    EmbeddedKafka.stop()
  }

  override def beforeAll(): Unit = {
    val default = EmbeddedKafkaConfig.defaultConfig
    val config = EmbeddedKafkaConfig(
      default.kafkaPort,
      default.zooKeeperPort,
      default.customBrokerProperties ++ Map("advertised.host.name" -> "PLAINTEXT://localhost", "listeners" -> s"PLAINTEXT://0.0.0.0:${default.kafkaPort}"),
      default.customProducerProperties,
      default.customConsumerProperties,
    )

    EmbeddedKafka.start()(config)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

}
