package base

import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import kafka.server.KafkaServer
import utils.RandomPort

abstract class KafkaSpecBase[F[_]](implicit F: Sync[F]) extends EmbeddedKafka {
  implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

  val embeddedKafka: Resource[F, KafkaServer] =
    Resource.make(F.delay(EmbeddedKafka.start().broker))(server => F.delay(server.shutdown()).void)

  def publishStringMessage(topic: String, key: String, message: String): F[Unit] =
    F.delay(publishToKafka(topic, key, message))

  def consumeStringMessage(topic: String, autoCommit: Boolean): F[String] =
    F.delay(consumeFirstStringMessageFrom(topic, autoCommit = autoCommit))
}
