package base

import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, IO, Resource}
import cats.syntax.all.*
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
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
        partitions: Int = testTopicPartitions
    ): NonEmptySet[TopicPartition] =
      topics.flatMap { topic =>
        createCustomTopic(topic = topic, partitions = partitions)
        NonEmptyList.fromListUnsafe((0 until partitions).toList).map(i => new TopicPartition(topic, i))
      }.toNes

    def records(r: Seq[Int]): Seq[(String, String)] = r.map(i => s"k$i" -> s"v$i")

    def recordToTuple[K, V](record: ConsumerRecord[K, V]): (K, V) = (record.key, record.value)
  }

  trait Consumer {
    this: TestContext =>

    def moveOffsetToEnd(partitions: NonEmptySet[TopicPartition]) =
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(true))
        .evalTap((consumer: KafkaConsumer[IO, Array[Byte], Array[Byte]]) =>
          for {
            _ <- consumer.assign(partitions)
            _ <- consumer.seekToEnd
            _ <- partitions.toList.traverse(consumer.position)
          } yield consumer
        )

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
