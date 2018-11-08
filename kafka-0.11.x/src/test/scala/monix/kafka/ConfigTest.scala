package monix.kafka

import org.scalatest.FunSuite

class ConfigTest extends FunSuite {
  test("overwrite properties with values from producer config") {
    val config =
      KafkaProducerConfig.default.copy(
        bootstrapServers = List("localhost:9092"),
        properties = Map("bootstrap.servers" -> "127.0.0.1:9092"))

    assert(
      config.toProperties.getProperty("bootstrap.servers") == "localhost:9092"
    )
  }

  test("overwrite properties with values from consumer config") {
    val config =
      KafkaConsumerConfig.default.copy(
        bootstrapServers = List("localhost:9092"),
        properties = Map("bootstrap.servers" -> "127.0.0.1:9092"))

    assert(
      config.toProperties.getProperty("bootstrap.servers") == "localhost:9092"
    )
  }
}
