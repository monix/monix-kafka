package monix.kafka.benchmarks

import monix.kafka.KafkaProducer
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{FlatSpec, Matchers}

class ProducerSpec extends FlatSpec with MonixFixture with Matchers  {

  val producer = KafkaProducer[String, String](producerConf, global)

  s"Monix ${topic_producer_1P_1RF}" should "exist befor running Producer Benchmark" in {
    val t = producer.send(topic = topic_producer_1P_1RF, "test")

    t.runSyncUnsafe().isDefined shouldBe true
  }

}
