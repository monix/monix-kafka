package monix.kafka.benchmarks

import monix.eval.Coeval

import scala.util.Random

trait BaseFixture {

  val brokerUrl = "127.0.0.1:9092"
  val randomId: Coeval[String] = Coeval(Random.alphanumeric.filter(_.isLetter).take(20).mkString)

  // topic names
  val monixTopic = "monix_topic"

}
