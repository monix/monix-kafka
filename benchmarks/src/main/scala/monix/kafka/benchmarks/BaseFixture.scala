package monix.kafka.benchmarks

import monix.eval.Coeval

import scala.util.Random

trait BaseFixture {

  val producerTestId = "producer"
  val sinkTestId = "sink"
  val consumerTestId = "consumer"
  val brokerUrl = "127.0.0.1:9092"

  val randomId: Coeval[String] = Coeval(Random.alphanumeric.filter(_.isLetter).take(20).mkString)
  def getTopicNames(testId: String): (String, String) = {
    val topic: (String, Int, Int) => String = {
      (testId, partitions: Int, replicationFactor: Int) =>
        //syntax (P, RF) === (Partitions, Replication Factor)
        s"topic_${testId}_${partitions}P_${replicationFactor}RF"
    }
    (topic(testId, 1, 1), topic(testId, 1, 2))
  }

  //monix-kafka benchmarks
  val (topic_producer_1P_1RF, topic_producer_2P_1RF) =  getTopicNames(producerTestId)
  val (topic_sink_1P_1RF, topic_sink_2P_1RF) = getTopicNames(sinkTestId)
  val (topic_consumer_1P_1RF, topic_consumer_2P_1RF) = getTopicNames(consumerTestId)

}
