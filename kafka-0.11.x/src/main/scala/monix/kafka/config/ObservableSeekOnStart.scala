package monix.kafka.config

import com.typesafe.config.ConfigException.BadValue

/** Specifies whether to call `seekToEnd` or `seekToBeginning` when starting
  * [[monix.kafka.KafkaConsumerObservable KafkaConsumerObservable]]
  *
  * Available options:
  *
  *  - [[ObservableSeekOnStart.End]]
  *  - [[ObservableSeekOnStart.Beginning]]
  *  - [[ObservableSeekOnStart.NoSeek]]
  */
sealed trait ObservableSeekOnStart extends Serializable {
  def id: String

  def isSeekBeginning: Boolean =
    this match {
      case ObservableSeekOnStart.Beginning => true
      case _ => false
    }

  def isSeekEnd: Boolean =
    this match {
      case ObservableSeekOnStart.End => true
      case _ => false
    }
}

object ObservableSeekOnStart {

  @throws(classOf[BadValue])
  def apply(id: String): ObservableSeekOnStart =
    id match {
      case End.id => End
      case Beginning.id => Beginning
      case NoSeek.id => NoSeek
      case _ =>
        throw new BadValue("kafka.monix.observable.seek.onStart", s"Invalid value: $id")
    }

  /** Calls `consumer.seekToEnd()` when starting consumer.
    */
  case object End extends ObservableSeekOnStart {
    val id = "end"
  }

  /** Calls `consumer.seekToBeginning()` when starting consumer.
    */
  case object Beginning extends ObservableSeekOnStart {
    val id = "beginning"
  }

  /** Does not call neither `consumer.seekToEnd()` nor `consumer.seekToBeginning`
    * when starting consumer.
    */
  case object NoSeek extends ObservableSeekOnStart {
    val id = "no-seek"
  }
}
