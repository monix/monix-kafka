package monix.kafka.config

import com.typesafe.config.ConfigException.BadValue

/** The `security.protocol` setting for the Kafka Producer.
  *
  * Represents the protocol used to communicate with brokers.
  *
  * Valid values are:
  *
  *  - [[SecurityProtocol.PLAINTEXT]]
  *  - [[SecurityProtocol.SSL]]
  *  - [[SecurityProtocol.SASL_PLAINTEXT]]
  *  - [[SecurityProtocol.SASL_SSL]]
  */
sealed trait SecurityProtocol extends Serializable {
  def id: String
}

object SecurityProtocol {
  @throws(classOf[BadValue])
  def apply(id: String): SecurityProtocol =
    id match {
      case PLAINTEXT.id => PLAINTEXT
      case SSL.id => SSL
      case SASL_PLAINTEXT.id => SASL_PLAINTEXT
      case SASL_SSL.id => SASL_SSL
      case _ =>
        throw new BadValue("kafka.security.protocol", s"Invalid value: $id")
    }

  case object PLAINTEXT extends SecurityProtocol {
    val id = "PLAINTEXT"
  }

  case object SSL extends SecurityProtocol {
    val id = "SSL"
  }

  case object SASL_PLAINTEXT extends SecurityProtocol {
    val id = "SASL_PLAINTEXT"
  }

  case object SASL_SSL extends SecurityProtocol {
    val id = "SASL_SSL"
  }
}
