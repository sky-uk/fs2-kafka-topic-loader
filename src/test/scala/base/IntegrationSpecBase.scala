package base

import cats.data.NonEmptyList
import cats.effect.{Async, IO, Resource}
import fs2.kafka.{AutoOffsetReset, ConsumerRecord, ConsumerSettings, KafkaConsumer}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import utils.RandomPort

import scala.concurrent.duration.*

abstract class IntegrationSpecBase extends UnitSpecBase {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(20.seconds, 200.millis)

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

    def createCustomTopics(topics: NonEmptyList[String], partitions: Int = testTopicPartitions): Unit =
      topics.toList.foreach(createCustomTopic(_, partitions = partitions))

    def records(r: Seq[Int]): Seq[(String, String)] = r.map(i => s"k$i" -> s"v$i")

    def recordToTuple[K, V](record: ConsumerRecord[K, V]): (K, V) = (record.key, record.value)
  }

  trait Consumer {
    this: TestContext =>

    def moveOffsetToEnd(topic: String) =
      KafkaConsumer
        .stream(consumerSettings.withEnableAutoCommit(true))
        .evalTap((consumer: KafkaConsumer[IO, Array[Byte], Array[Byte]]) =>
          for {
            _ <- consumer.subscribeTo(topic)
            _ <- consumer.seekToEnd
          } yield consumer // TODO - make this consume one record then stop
        )
        .records
        .take(1)
        .drain

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
