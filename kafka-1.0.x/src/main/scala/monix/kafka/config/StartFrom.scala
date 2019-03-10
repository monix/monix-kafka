package monix.kafka.config

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

sealed trait StartFrom extends Serializable

object StartFrom {

  /** Automatically reset the offset to the earliest offset. */
  case object FromEarliest extends StartFrom

  /** Automatically reset the offset to the latest offset. */
  case object FromLatest extends StartFrom

  /** Uses the current offset or falls back on the [[AutoOffsetReset]] setting
    */
  case object FromCommited extends StartFrom

  /** Resets the offset to an offset starting from now minus the period
    * @param period the period which we will seek back from
    */
  case class PriorToNow(period: FiniteDuration) extends StartFrom

  /** Resets the offset to an offset at a certain time
    * @param instant the time where the offset will start from
    */
  case class StartAt(instant: Instant) extends StartFrom
}
