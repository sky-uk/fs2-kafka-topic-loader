package utils

import cats.Show
import cats.effect.Sync
import cats.syntax.all.*
import org.apache.kafka.common.security.auth.SecurityProtocol

final case class KafkaConfig(
    protocol: SecurityProtocol,
    host: String,
    kafkaPort: Int,
    controllerPort: Int
) {
  val plaintextListener = s"$protocol://$host:$kafkaPort"
}

object KafkaConfig {
  def random[F[_] : Sync]: F[KafkaConfig] =
    for {
      kafkaPort      <- RandomPort[F]
      controllerPort <- RandomPort[F]
    } yield KafkaConfig(
      protocol = SecurityProtocol.PLAINTEXT,
      host = "localhost",
      kafkaPort = kafkaPort,
      controllerPort = controllerPort
    )

  given Show[KafkaConfig] = Show.fromToString
}
