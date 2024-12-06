package utils

import java.util.UUID

import base.AsyncIntSpec
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion
import org.scalatest.concurrent.{AbstractPatienceConfiguration, Eventually}
import org.scalatest.exceptions.TestFailedException
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import uk.sky.fs2.kafka.topicloader.{LoadTopicStrategy, TopicLoader}
import utils.KafkaContainer.KafkaConfig

import scala.concurrent.duration.*

trait KafkaHelpers[F[_]] {
  self: AsyncIntSpec[F] & EmbeddedKafka[F] & AbstractPatienceConfiguration =>

  val groupId    = "test-consumer-group"
  val testTopic1 = "load-state-topic-1"
  val testTopic2 = "load-state-topic-2"

  given consumerSettings(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withGroupId(groupId)

  val aggressiveCompactionConfig = Map(
    "cleanup.policy"            -> "compact",
    "delete.retention.ms"       -> "0",
    "min.cleanable.dirty.ratio" -> "0.01",
    "segment.ms"                -> "1"
  )

  val aggressiveDeletionConfig = Map(
    "cleanup.policy"            -> "delete",
    "delete.retention.ms"       -> "0",
    "retention.ms"              -> "0",
    "min.cleanable.dirty.ratio" -> "0.01",
    "segment.ms"                -> "1"
  )

  def records(r: Seq[Int]): Seq[(String, String)] = r.map(i => s"k$i" -> s"v$i")

  def recordToTuple[K, V](record: ConsumerRecord[K, V]): (K, V) = (record.key, record.value)

  /*
   * Note: Compaction is only triggered if messages are published as a separate statement.
   */
  def publishToKafkaAndTriggerCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = {
    val topic      = partitions.map(_.topic()).toList.head
    val fillerSize = 100
    val filler     = List.fill(fillerSize)(UUID.randomUUID().toString).map(x => (x, x))

    publishStringMessages(topic, messages) *> publishStringMessages(topic, filler)
  }

  def runLoader(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy
  )(using consumerSettings: ConsumerSettings[F, String, String], F: Async[F]): F[List[(String, String)]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader
      .load(topics, strategy, consumerSettings)
      .compile
      .toList
      .map(_.map(recordToTuple))
      .timeoutTo(
        patienceConfig.timeout,
        F.raiseError(TestFailedException(s"TopicLoader did not complete after ${patienceConfig.timeout.toSeconds}s", 0))
      )
  }

  def runLoaderChunks(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      process: ConsumerRecord[String, String] => F[Unit]
  )(using consumerSettings: ConsumerSettings[F, String, String], F: Async[F]): F[Unit] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]

    TopicLoader
      .loadChunks(topics, strategy, consumerSettings)(_.traverse_(process))
      .timeoutTo(
        patienceConfig.timeout,
        F.raiseError(TestFailedException(s"TopicLoader did not complete after ${patienceConfig.timeout.toSeconds}s", 0))
      )
  }

  def loadAndRunR(topics: NonEmptyList[String])(
      onLoad: Resource.ExitCase => F[Unit],
      onRecord: ((String, String)) => F[Unit]
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Resource[F, Unit] =
    Supervisor[F]
      .evalMap(_.supervise {
        loadAndRunLoader(topics)(onLoad)
          .map(recordToTuple)
          .evalTap(onRecord)
          .compile
          .drain
      })
      .void

  def loadAndRunChunksR(topics: NonEmptyList[String])(
      onLoad: Resource.ExitCase => F[Unit],
      onRecord: ((String, String)) => F[Unit]
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Resource[F, Unit] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]

    Supervisor[F]
      .evalMap(_.supervise {
        TopicLoader
          .loadAndRunChunks(
            topics,
            consumerSettings
          )(
            onLoad
          )(
            _.map(recordToTuple).map(onRecord).sequence_
          )
      })
      .void
  }

  def loadAndRunLoader(topics: NonEmptyList[String])(onLoad: Resource.ExitCase => F[Unit])(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Stream[F, ConsumerRecord[String, String]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader.loadAndRun(topics, consumerSettings)(onLoad)
  }

  def moveOffsetToEnd(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] =
    withAssignedConsumer(
      autoCommit = true,
      offsetReset = AutoOffsetReset.Earliest,
      partitions = partitions,
      groupId = groupId
    ) { consumer =>
      for {
        endOffsets         <- consumer.endOffsets(partitions.toSortedSet)
        offsetsAndMetadata <- F.pure(endOffsets.view.mapValues(OffsetAndMetadata(_, "")).toMap)
        _                  <- consumer.commitSync(offsetsAndMetadata)
      } yield ()
    }

  def publishToKafkaAndWaitForCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForCompaction(partitions)
  } yield ()

  def publishToKafkaAndWaitForDeletion(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForDeletion(partitions)
  } yield ()

  def waitForCompaction(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    consumeEventually(partitions) { records =>
      val messageKeys = records.map((k, _) => k)
      messageKeys should not be empty
      messageKeys should contain theSameElementsAs messageKeys.toSet

    }

  def waitForDeletion(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    consumeEventually(partitions) { records =>
      records shouldBe empty
    }

  def consumeEventually(
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(
      f: List[(String, String)] => Assertion
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Assertion] =
    eventually {
      val records: F[List[(String, String)]] = withAssignedConsumer(
        autoCommit = false,
        offsetReset = AutoOffsetReset.Earliest,
        partitions = partitions,
        groupId = groupId
      )(_.records.map(_.record).map(recordToTuple).interruptAfter(5.second).compile.toList)

      records.map(f)
    }

  def withAssignedConsumer[T](
      autoCommit: Boolean,
      offsetReset: AutoOffsetReset,
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(f: KafkaConsumer[F, String, String] => F[T])(using kafkaConfig: KafkaConfig, F: Async[F]): F[T] =
    withConsumer(autoCommit, offsetReset, groupId)(consumer => consumer.assign(partitions) >> f(consumer))
}
