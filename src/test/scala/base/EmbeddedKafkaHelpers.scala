package base

import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.EmbeddedKafka.*
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import kafka.server.KafkaServer
import org.apache.kafka.common.TopicPartition

trait EmbeddedKafkaHelpers[F[_]] {
  def embeddedKafka(implicit kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): Resource[F, KafkaServer] =
    Resource.make(F.delay(EmbeddedKafka.start().broker))(server => F.delay(server.shutdown()).void)

  val testTopicPartitions = 5

  implicit object TopicPartitionOrder extends Order[TopicPartition] {
    override def compare(x: TopicPartition, y: TopicPartition): Int = x.hashCode().compareTo(y.hashCode())
  }

  def createCustomTopics(
      topics: NonEmptyList[String],
      partitions: Int = testTopicPartitions,
      topicConfig: Map[String, String] = Map.empty
  )(implicit kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[NonEmptySet[TopicPartition]] =
    F.delay(topics.flatMap { topic =>
      createCustomTopic(topic = topic, topicConfig = topicConfig, partitions = partitions)
      NonEmptyList.fromListUnsafe((0 until partitions).toList).map(i => new TopicPartition(topic, i))
    }.toNes)

  def publishStringMessage(topic: String, key: String, message: String)(implicit
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] =
    F.delay(publishToKafka(topic, key, message))

  def publishStringMessages(topic: String, messages: Seq[(String, String)])(implicit
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] =
    messages.traverse { case (k, v) => publishStringMessage(topic, k, v) }.void

  def consumeStringMessage(topic: String, autoCommit: Boolean)(implicit
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[String] =
    F.delay(consumeFirstStringMessageFrom(topic, autoCommit = autoCommit))
}
