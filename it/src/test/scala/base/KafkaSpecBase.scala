package base

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import utils.RandomPort

abstract class KafkaSpecBase[F[_]](implicit F: Sync[F])
    extends AsyncWordSpec
    with AsyncIOSpec
    with Matchers
    with OptionValues
    with EmbeddedKafka {
  implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

  val embeddedKafka: Resource[F, Unit] =
    Resource.make(F.delay(EmbeddedKafka.start()).void)(_ => F.delay(EmbeddedKafka.stop()).void)

  def publishStringMessage(topic: String, key: String, message: String): F[Unit] =
    F.delay(publishToKafka(topic, key, message))

  def consumeStringMessage(topic: String, autoCommit: Boolean): F[String] =
    F.delay(consumeFirstStringMessageFrom(topic, autoCommit = autoCommit))
}
