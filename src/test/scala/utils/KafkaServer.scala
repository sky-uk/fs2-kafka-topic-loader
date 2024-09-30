package utils

import java.io.{File, OutputStream, PrintStream}
import java.nio.file.{Files, Path}

import cats.Show
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.effect.{Async, Ref, Resource, Sync}
import cats.syntax.all.*
import kafka.server.{KafkaConfig as JKafkaConfig, KafkaRaftServer}
import kafka.tools.StorageTool
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.typelevel.log4cats.syntax.*
import org.typelevel.log4cats.{Logger, LoggerFactory}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object KafkaServer {
  sealed trait Running[F[_]] {
    def stop: F[Unit]
  }

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

  def start[F[_] : LoggerFactory](kafkaConfig: KafkaConfig)(using F: Async[F]): F[KafkaServer.Running[F]] =
    for {
      given Logger[F] <- LoggerFactory[F].create
      _               <- debug"Created config ${kafkaConfig.show}"
      logDir          <- F.blocking(Files.createTempDirectory("embedded-kafka"))
      _               <- debug"Created log directory ${logDir.toAbsolutePath.toString}"
      jKafkaConfig    <- F.pure(JKafkaConfig(properties(logDir, kafkaConfig).widen[Object].asJava))
      clusterId       <- UUIDGen[F].randomUUID.map(uuid => Uuid(uuid.getLeastSignificantBits, uuid.getMostSignificantBits))
      _               <- formatLogDirectory(clusterId, jKafkaConfig)
      time            <- F.delay(Time.SYSTEM)
      state           <- Ref.of(State.Starting)
      server          <- F.blocking(KafkaRaftServer(jKafkaConfig, time))
      _               <- F.blocking(server.startup())
      _               <- debug"Server started"
      _               <- state.set(State.Started)
    } yield new KafkaServer.Running[F] {
      override def stop: F[Unit] = {
        val deleteLogDir =
          debug"Deleting log directory ${logDir.toAbsolutePath.toString}" >>
            delete(logDir.toFile).attempt.flatMap {
              case Right(_)    => debug"Deleted log directory successfully"
              case Left(error) => Logger[F].error(error)(s"Could not delete log directory: $error")
            }

        val shutdown = for {
          _ <- debug"Starting shutdown"
          _ <- F.blocking(server.shutdown)
          _ <- debug"Awaiting shutdown"
          _ <- F.interruptible(server.awaitShutdown)
                 .timeoutTo(30.seconds, TimeoutException("Could not shutdown within 30 seconds").raiseError)
        } yield ()

        for {
          currentState <- state.getAndSet(State.Stopping)
          _            <- currentState match {
                            case State.Started | State.Stopping => F.unit
                            case State.Starting                 => warn"Server cannot be shutdown while starting"
                            case State.Stopped                  => IllegalStateException("Server is already stopped").raiseError
                          }
          _            <- shutdown.guarantee(deleteLogDir).guarantee(state.set(State.Stopped))
        } yield ()
      }
    }

  def resource[F[_] : LoggerFactory](kafkaConfig: KafkaConfig)(using F: Async[F]): Resource[F, KafkaServer.Running[F]] =
    Resource.make(start[F](kafkaConfig))(_.stop)

  private def properties(logDir: Path, kafkaConfig: KafkaConfig): Map[String, String] = {
    val controllerListener: String = s"CONTROLLER://localhost:${kafkaConfig.controllerPort}"
    val replicationFactor          = "1"

    Map[String, String](
      CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP                  -> "1048577",
      ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG                     -> "true",
      GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG             -> "1",
      GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG     -> replicationFactor,
      KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG                      -> "CONTROLLER",
      KRaftConfigs.PROCESS_ROLES_CONFIG                                  -> "broker,controller",
      QuorumConfig.QUORUM_VOTERS_CONFIG                                  -> s"0@localhost:${kafkaConfig.controllerPort}",
      ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG               -> "PLAINTEXT",
      ServerConfigs.BROKER_ID_CONFIG                                     -> "0",
      ServerLogConfigs.LOG_DIRS_CONFIG                                   -> logDir.toAbsolutePath.toString,
      ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG                -> "1",
      SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG                    -> kafkaConfig.plaintextListener,
      SocketServerConfigs.LISTENERS_CONFIG                               -> s"${kafkaConfig.plaintextListener},$controllerListener",
      SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG          -> "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
      TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG            -> "1",
      TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG -> replicationFactor
    )
  }

  private def formatLogDirectory[F[_]](clusterId: Uuid, kafkaConfig: JKafkaConfig)(using F: Sync[F]): F[Int] =
    F.blocking(
      StorageTool.formatCommand(
        stream = PrintStream(OutputStream.nullOutputStream()),
        directories = kafkaConfig.logDirs.toSeq,
        metaProperties = StorageTool.buildMetadataProperties(clusterId.toString, kafkaConfig),
        metadataVersion = MetadataVersion.fromVersionString(kafkaConfig.interBrokerProtocolVersionString),
        ignoreFormatted = true
      )
    )

  private def delete[F[_] : Logger](file: File)(using F: Sync[F]): F[Unit] = {
    def deleteDir(file: File): F[Unit] =
      for {
        files <- F.blocking(file.listFiles().toList)
        _     <- debug"recursively deleting ${files.map(_.getPath).mkString_(", ")}"
        _     <- files.traverse_(delete)
        _     <- debug"Deleting directory ${file.getPath}"
        _     <- F.blocking(file.delete).void
      } yield ()

    for {
      isDirectory <- F.blocking(file.isDirectory)
      _           <- if (isDirectory) deleteDir(file)
                     else debug"Deleting file ${file.getPath}" >> F.blocking(file.delete()).void
    } yield ()
  }

  private enum State {
    case Starting, Started, Stopping, Stopped
  }
}
