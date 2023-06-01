package base

import io.github.embeddedkafka.EmbeddedKafkaConfig
import utils.RandomPort

abstract class TestContext[F[_]] {
  implicit val kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))
}
