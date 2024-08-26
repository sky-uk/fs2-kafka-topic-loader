package utils

import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import com.dimafeng.testcontainers.KafkaContainer
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

trait KafkaTestContainer[F[_]] {
  def resource(using F: Sync[F]): Resource[F, (KafkaContainer, EmbeddedKafkaConfig)] =
    Resource.make {
      for {
        container <- F.pure(KafkaContainer())
        _         <- F.blocking(container.start())
        port      <- F.defer(
                       container.bootstrapServers
                         .split(":")
                         .lastOption
                         .flatMap(_.toIntOption)
                         .liftTo(TestFailedException(s"Could not obtain a port from ${container.bootstrapServers}", 0))
                     )
      } yield {
        val config = EmbeddedKafkaConfig(kafkaPort = port, customBrokerProperties = Map("log.roll.ms" -> "10"))
        (container, config)
      }
    }((container, _) => F.blocking(container.stop()))

  def withKafkaContext(test: EmbeddedKafkaConfig => F[Assertion])(using Sync[F]): F[Assertion] =
    resource.use((_, config) => test(config))

}
