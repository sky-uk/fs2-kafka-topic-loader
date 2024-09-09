package utils

import cats.effect.{Ref, Resource, Sync}
import cats.syntax.all.*
import org.scalatest.Assertion
import org.testcontainers.containers.KafkaContainer as Underlying
import org.testcontainers.utility.DockerImageName
import utils.KafkaContainer.KafkaConfig

trait KafkaTestContainer[F[_]] {
  def withKafkaContext(test: KafkaConfig => F[Assertion])(using Sync[F]): F[Assertion] =
    KafkaContainer[F].use(running => test(running.config))
}

object KafkaContainer {
  final case class KafkaConfig(kafkaPort: Int)

  sealed trait Running[F[_]] {
    def config: KafkaConfig
    def stop: F[Unit]
  }

  object Running {
    inline def apply[F[_]](using ev: Running[F]): Running[F] = ev
  }

  def apply[F[_]](using F: Sync[F]): Resource[F, Running[F]] =
    Resource.make {
      for {
        container <- F.pure(Underlying(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")))
        _         <- F.blocking(container.start())
        _         <- F.delay(println(s"bootstrap servers: ${container.getBootstrapServers}"))
        foundPort <-
          F.defer(
            container.getBootstrapServers
              .split(":")
              .lastOption
              .flatMap(_.toIntOption)
              .liftTo(IllegalStateException(s"Could not obtain a port from ${container.getBootstrapServers}"))
          )
        _         <- F.delay(println(s"Port: $foundPort"))
        stopped   <- Ref[F].of(false)
      } yield new Running[F] {
        override def config: KafkaConfig = KafkaConfig(kafkaPort = foundPort)

        override def stop: F[Unit] =
          stopped
            .getAndSet(true)
            .flatMap(stopped =>
              if (stopped) RuntimeException("Container has already been shutdown").raiseError
              else F.blocking(container.stop())
            )
      }
    }(_.stop)
}
