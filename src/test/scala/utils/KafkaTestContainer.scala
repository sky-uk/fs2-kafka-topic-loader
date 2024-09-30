package utils

import cats.effect.std.Env
import cats.effect.syntax.all.*
import cats.effect.{Ref, Resource, Sync}
import cats.syntax.all.*
import org.scalatest.Assertion
import org.testcontainers.containers.KafkaContainer as Underlying
import org.testcontainers.utility.DockerImageName
import utils.KafkaContainer.KafkaConfig

trait KafkaTestContainer[F[_]] {
  def withRunningKafka(test: KafkaConfig => F[Assertion])(using Sync[F], Env[F]): F[Assertion] =
    KafkaContainer[F].use(running => test(running.config))
}

object KafkaContainer {
  sealed trait Running[F[_]] {
    def config: KafkaConfig
    def stop: F[Unit]
  }

  def apply[F[_] : Env](using F: Sync[F]): Resource[F, Running[F]] =
    Resource.make {
      for {
        status           <- Ref.of(Status.Starting)
        env              <- Env[F].get("CONFLUENT_KAFKA_VERSION")
        container        <- Either
                              .catchNonFatal(
                                Underlying(DockerImageName.parse(s"confluentinc/cp-kafka:${env.getOrElse("7.4.0")}"))
                              )
                              .liftTo[F]
        _                <- F.blocking(container.start())
        _                <- status.set(Status.Started)
        bootstrapServers <- F.blocking(container.getBootstrapServers)
        kafkaConfig      <- KafkaConfig(bootstrapServers).liftTo[F]
      } yield new Running[F] {
        override def config: KafkaConfig = kafkaConfig

        override def stop: F[Unit] =
          status.getAndSet(Status.Stopping).flatMap {
            case Status.Starting | Status.Started => F.blocking(container.stop()).guarantee(status.set(Status.Stopped))
            case Status.Stopping                  => F.unit
            case Status.Stopped                   => RuntimeException("Container has already been shutdown").raiseError
          }
      }
    }(_.stop)

  final case class KafkaConfig(protocol: String, host: String, kafkaPort: Int) {
    val bootstrapServer = s"$protocol://$host:$kafkaPort"
  }

  object KafkaConfig {
    def apply(bootstrapServers: String): Either[Throwable, KafkaConfig] =
      bootstrapServers match {
        case s"$protocol://$host:$port" =>
          port.toIntOption
            .map(KafkaConfig(protocol, host, _))
            .toRight(RuntimeException(s"$bootstrapServers did not have a valid port: $port"))

        case _ =>
          RuntimeException(s"Unable to extract host and port from $bootstrapServers").asLeft
      }
  }

  private enum Status {
    case Starting, Started, Stopping, Stopped
  }
}
