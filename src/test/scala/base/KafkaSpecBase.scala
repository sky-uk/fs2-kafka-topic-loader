package base

import java.util.UUID
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.*

import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest.exceptions.TestFailedException
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import uk.sky.fs2.kafka.topicloader.{LoadTopicStrategy, TopicLoader}

import scala.concurrent.duration.*

abstract class KafkaSpecBase[F[_]](implicit F: Async[F]) extends AsyncIntSpecBase {
  //  implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
  //    EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

  private val embeddedKafkaHelpers = new EmbeddedKafkaHelpers[F] {}
  import embeddedKafkaHelpers.*

  private implicit val loggerFactory: LoggerFactory[F] = Slf4jFactory.create[F]

  val groupId    = "test-consumer-group"
  val testTopic1 = "load-state-topic-1"
  val testTopic2 = "load-state-topic-2"

  implicit def consumerSettings(implicit kafkaConfig: EmbeddedKafkaConfig): ConsumerSettings[F, String, String] =
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
  )(implicit kafkaConfig: EmbeddedKafkaConfig): F[Unit] = {
    val topic      = partitions.map(_.topic()).toList.head
    val fillerSize = 20
    val filler     = List.fill(fillerSize)(UUID.randomUUID().toString).map(x => (x, x))

    publishStringMessages(topic, messages) *> publishStringMessages(topic, filler)
  }

  def runLoader(
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy
  )(implicit consumerSettings: ConsumerSettings[F, String, String]): F[List[(String, String)]] =
    TopicLoader.load(topics, strategy, consumerSettings).compile.toList.map(_.map(recordToTuple))

  def moveOffsetToEnd(
      partitions: NonEmptySet[TopicPartition]
  )(implicit kafkaConfig: EmbeddedKafkaConfig): Stream[F, KafkaConsumer[F, String, String]] =
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
  )(implicit kafkaConfig: EmbeddedKafkaConfig): F[Unit] = for {
    _ <- publishToKafkaAndTriggerCompaction(partitions, messages)
    _ <- waitForCompaction(partitions)
  } yield ()

  def waitForCompaction(partitions: NonEmptySet[TopicPartition])(implicit kafkaConfig: EmbeddedKafkaConfig): F[Unit] =
    consumeEventually(partitions) { r =>
      for {
        records    <- r
        messageKeys = records.map { case (k, _) => k }
        result     <-
          if (messageKeys.sorted == messageKeys.toSet.toList.sorted) F.unit
          else F.raiseError(new TestFailedException("Topic has not compacted within timeout", 1))
      } yield result
    }

  def consumeEventually(
      partitions: NonEmptySet[TopicPartition],
      groupId: String = UUID.randomUUID().toString
  )(
      f: F[List[(String, String)]] => F[Unit]
  )(implicit kafkaConfig: EmbeddedKafkaConfig): F[Unit] =
    retry(
      fa = {
        val records = withAssignedConsumer[F[List[ConsumerRecord[String, String]]]](
          autoCommit = false,
          offsetReset = AutoOffsetReset.Earliest,
          partitions,
          groupId.some
        )(
          _.records
            .map(_.record)
            .interruptAfter(5.second)
            .compile
            .toList
        )

        f(records.map(_.map(r => r.key -> r.value)))
      },
      delay = 1.second,
      max = 5
    )

  def withAssignedConsumer[T](
      autoCommit: Boolean,
      offsetReset: AutoOffsetReset,
      partitions: NonEmptySet[TopicPartition],
      groupId: Option[String] = None
  )(f: Stream[F, KafkaConsumer[F, String, String]] => T)(implicit kafkaConfig: EmbeddedKafkaConfig): T = {
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
  )(implicit kafkaConfig: EmbeddedKafkaConfig): Resource[F, KafkaConsumer[F, String, String]] = {
    val baseSettings =
      ConsumerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withEnableAutoCommit(autoCommit)
        .withAutoOffsetReset(offsetReset)

    val settings = groupId.fold(baseSettings)(baseSettings.withGroupId)
    KafkaConsumer[F].resource(settings)
  }

  def retry[A](fa: F[A], delay: FiniteDuration, max: Int): F[A] =
    if (max <= 1) fa
    else
      fa handleErrorWith { _ =>
        F.sleep(delay) *> retry(fa, delay, max - 1)
      }
}
