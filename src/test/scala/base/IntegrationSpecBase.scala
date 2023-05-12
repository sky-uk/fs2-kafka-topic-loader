package base

import java.util.UUID

import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Resource}
import cats.syntax.all.*
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest.Assertion
import org.scalatest.LoneElement.*
import utils.RandomPort

import scala.concurrent.duration.*

abstract class IntegrationSpecBase extends UnitSpecBase {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(20.seconds, 200.millis)

  implicit object TopicPartitionOrder extends Order[TopicPartition] {
    override def compare(x: TopicPartition, y: TopicPartition): Int = x.hashCode().compareTo(y.hashCode())
  }

  trait TestContext extends EmbeddedKafka {

    implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

    val consumerSettings: ConsumerSettings[IO, Array[Byte], Array[Byte]] =
      ConsumerSettings[IO, Array[Byte], Array[Byte]]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("test-consumer-group")

    val testTopic1          = "load-state-topic-1"
    val testTopic2          = "load-state-topic-2"
    val testTopicPartitions = 5

    def createCustomTopics(
        topics: NonEmptyList[String],
        partitions: Int = testTopicPartitions,
        topicConfig: Map[String, String] = Map.empty
    ): NonEmptySet[TopicPartition] =
      topics.flatMap { topic =>
        createCustomTopic(topic = topic, topicConfig = topicConfig, partitions = partitions)
        NonEmptyList.fromListUnsafe((0 until partitions).toList).map(i => new TopicPartition(topic, i))
      }.toNes

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
    ): Unit = {
      val topic      = partitions.map(_.topic()).toList.loneElement
      val fillerSize = 20
      val filler     = List.fill(fillerSize)(UUID.randomUUID().toString).map(x => (x, x))

      publishToKafka(topic, messages)
      publishToKafka(topic, filler)
    }
  }

  trait Consumer { this: TestContext =>

    def publishToKafkaAndWaitForCompaction(
        partitions: NonEmptySet[TopicPartition],
        messages: Seq[(String, String)]
    ): Assertion = {
      publishToKafkaAndTriggerCompaction(partitions, messages)
      waitForCompaction(partitions)
    }

    def moveOffsetToEnd(partitions: NonEmptySet[TopicPartition]) =
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(true))
        .evalTap(consumer =>
          for {
            _ <- consumer.assign(partitions)
            _ <- consumer.seekToEnd
            _ <- partitions.toList.traverse(consumer.position)
          } yield consumer
        )

    // TODO - split this out
    def waitForCompaction(partitions: NonEmptySet[TopicPartition]): Assertion = {
      val consumerSettings = ConsumerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId(UUID.randomUUID().toString)

      eventually {
        val messages = KafkaConsumer
          .stream(consumerSettings)
          .evalTap(consumer =>
            for {
              _ <- consumer.assign(partitions)
              _ <- consumer.seekToBeginning
            } yield consumer
          )
          .records
          .map(_.record)
          .interruptAfter(1.second)
          .compile
          .toList
          .unsafeRunSync()

        val records     = messages.map(r => r.key -> r.value)
        val messageKeys = records.map { case (k, _) => k }
        println(s"non-compacted messages: $messageKeys")
        messageKeys should contain theSameElementsAs messageKeys.toSet
      }
    }
    //      consumeEventually(topic) { r =>
//        val messageKeys = r.map { case (k, _) => k }
//        messageKeys should contain theSameElementsAs messageKeys.toSet
//      }
//
//    def consumeEventually(topic: String, groupId: String = UUID.randomUUID().toString)(
//        f: List[(String, String)] => Assertion
//    ): Assertion =
//      eventually {
//        val records = withAssignedConsumer(autoCommit = false, offsetReset = "earliest", topic, groupId.some)(
//          consumeAllKafkaRecordsFromEarliestOffset(_, List.empty)
//        )
//
//        f(records.map(r => r.key -> r.value))
//      }

    def createConsumer[F[_] : Async](
        autoCommit: Boolean,
        offsetReset: String,
        groupId: Option[String]
    ): Resource[F, KafkaConsumer[F, String, String]] = {
      val baseSettings =
        ConsumerSettings[F, String, String]
          .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)
          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)

      val settings = groupId.fold(baseSettings)(baseSettings.withGroupId)
      KafkaConsumer[F].resource(settings)
    }
  }

}
