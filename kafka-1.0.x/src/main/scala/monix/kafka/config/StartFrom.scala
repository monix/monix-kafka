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
  case object FromCommitted extends StartFrom

  /** Resets the offset to an offset starting from now minus the period
    * @param period the period which we will seek back from
    */
  def PriorToNow(period: FiniteDuration, time:Instant = Instant.now()): StartFrom = {
    StartAt(time.minusMillis(period.toMillis))
  }

  /** Resets the offset to an offset at a certain time
    * @param instant the time where the offset will start from
    */
  final case class StartAt(instant: Instant) extends StartFrom
}
