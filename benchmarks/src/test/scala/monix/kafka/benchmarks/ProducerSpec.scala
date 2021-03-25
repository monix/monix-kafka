package monix.kafka.benchmarks

import monix.kafka.KafkaProducer
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{FlatSpec, Matchers}

class ProducerSpec extends FlatSpec with MonixFixture with Matchers  {

  val producer = KafkaProducer[String, String](producerConf, global)


}
