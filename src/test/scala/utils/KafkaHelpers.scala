package utils

import java.util.UUID

import base.AsyncIntSpec
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.Fiber
import cats.effect.syntax.all.*
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import uk.sky.fs2.kafka.topicloader.{LoadTopicStrategy, TopicLoader}

import scala.concurrent.duration.*

trait KafkaHelpers[F[_]] {
  self: AsyncIntSpec[F] & EmbeddedKafka[F] =>

  val groupId    = "test-consumer-group"
  val testTopic1 = "load-state-topic-1"
  val testTopic2 = "load-state-topic-2"

  given consumerSettings(using
      kafkaConfig: EmbeddedKafkaConfig,
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

  def records(r: Seq[Int]): Seq[(String, String)] = r.map(i => s"k$i" -> s"v$i")

  def recordToTuple[K, V](record: ConsumerRecord[K, V]): (K, V) = (record.key, record.value)

  /*
   * Note: Compaction is only triggered if messages are published as a separate statement.
   */
  def publishToKafkaAndTriggerCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[Unit] = {
    val topic      = partitions.map(_.topic()).toList.head
    val fillerSize = 20
    val filler     = List.fill(fillerSize)(UUID.randomUUID().toString).map(x => (x, x))

    publishStringMessages(topic, messages) *> publishStringMessages(topic, filler)
  }

  def runLoader(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy
  )(using consumerSettings: ConsumerSettings[F, String, String], F: Async[F]): F[List[(String, String)]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader.load(topics, strategy, consumerSettings).compile.toList.map(_.map(recordToTuple))
  }

  def loadAndRunR(topics: NonEmptyList[String])(
      onLoad: Resource.ExitCase => F[Unit],
      onRecord: ((String, String)) => F[Unit]
  )(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Resource[F, Fiber[F, Throwable, Unit]] = Resource.make {
    loadAndRunLoader(topics)(onLoad)
      .map(recordToTuple)
      .evalTap(onRecord)
      .compile
      .drain
      .start
  }(_.cancel.void)

  def loadAndRunLoader(topics: NonEmptyList[String])(onLoad: Resource.ExitCase => F[Unit])(using
      consumerSettings: ConsumerSettings[F, String, String],
      F: Async[F]
  ): Stream[F, ConsumerRecord[String, String]] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    TopicLoader.loadAndRun(topics, consumerSettings)(onLoad)
  }

  def moveOffsetToEnd(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): Stream[F, KafkaConsumer[F, String, String]] =
    withAssignedConsumer(
      autoCommit = true,
      offsetReset = AutoOffsetReset.Latest,
      partitions = partitions,
      groupId = groupId.some
    )(
      _.evalMap(consumer =>
        for {
          _ <- consumer.seekToEnd
          _ <- partitions.toList.traverse(consumer.position)
        } yield consumer
      )
    )

  def publishToKafkaAndWaitForCompaction(
      partitions: NonEmptySet[TopicPartition],
      messages: Seq[(String, String)]
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForCompaction(partitions)
  } yield ()

  def waitForCompaction(
      partitions: NonEmptySet[TopicPartition]
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[Assertion] =
    consumeEventually(partitions) { r =>
      for {
        records    <- r
        messageKeys = records.map { case (k, _) => k }
      } yield {
        messageKeys should not be empty
        messageKeys should contain theSameElementsAs messageKeys.toSet
      }
    }

  def consumeEventually(
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(
      f: F[List[(String, String)]] => F[Assertion]
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[Assertion] =
    eventually {
      val records = withAssignedConsumer[F[List[ConsumerRecord[String, String]]]](
        autoCommit = false,
        offsetReset = AutoOffsetReset.Earliest,
        partitions,
        groupId.some
      )(_.records.map(_.record).interruptAfter(5.second).compile.toList)

      f(records.map(_.map(r => r.key -> r.value)))
    }

  def withAssignedConsumer[T](
      autoCommit: Boolean,
      offsetReset: AutoOffsetReset,
      partitions: NonEmptySet[TopicPartition],
      groupId: Option[String] = None
  )(f: Stream[F, KafkaConsumer[F, String, String]] => T)(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): T = {
    val consumer = createConsumer(autoCommit, offsetReset, groupId)

    val stream = Stream
      .resource(consumer)
      .evalMap(c =>
        for {
          _ <- c.assign(partitions)
          _ <- c.seekToBeginning
        } yield c
      )

    f(stream)
  }

  def createConsumer(
      autoCommit: Boolean,
      offsetReset: AutoOffsetReset,
      groupId: Option[String]
  )(implicit kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): Resource[F, KafkaConsumer[F, String, String]] = {
    val baseSettings =
      ConsumerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withEnableAutoCommit(autoCommit)
        .withAutoOffsetReset(offsetReset)

    val settings = groupId.fold(baseSettings)(baseSettings.withGroupId)
    KafkaConsumer[F].resource(settings)
  }
}
