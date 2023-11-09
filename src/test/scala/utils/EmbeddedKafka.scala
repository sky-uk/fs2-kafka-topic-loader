package utils

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka as Underlying, EmbeddedKafkaConfig}
import kafka.server.KafkaServer
import org.apache.kafka.common.TopicPartition
import uk.sky.fs2.kafka.topicloader.TopicLoader.topicPartitionOrder

trait EmbeddedKafka[F[_]] {

  def embeddedKafkaConfigF(implicit F: Sync[F]): F[EmbeddedKafkaConfig] = for {
    kafkaPort     <- RandomPort[F]
    zooKeeperPort <- RandomPort[F]
  } yield EmbeddedKafkaConfig(kafkaPort, zooKeeperPort, customBrokerProperties = Map("log.roll.ms" -> "10"))

  def embeddedKafkaR(kafkaConfig: EmbeddedKafkaConfig)(using F: Async[F]): Resource[F, KafkaServer] =
    Resource.make(F.blocking(Underlying.start()(kafkaConfig).broker))(server => F.blocking(server.shutdown()).void)

  def createCustomTopic(topic: String, partitions: Int, topicConfig: Map[String, String])(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[NonEmptyList[TopicPartition]] =
    for {
      tpIndexes   <- F.fromOption(
                       NonEmptyList.fromList((0 until partitions).toList),
                       IllegalStateException(s"Partitions cannot be < 1 - got $partitions")
                     )
      maybeCreate <- F.blocking {
                       Underlying.createCustomTopic(topic = topic, topicConfig = topicConfig, partitions = partitions)
                     }
      _           <- F.fromTry(maybeCreate)
    } yield tpIndexes.map(TopicPartition(topic, _))

  def createCustomTopics(
      topics: NonEmptyList[String],
      partitions: Int = 2,
      topicConfig: Map[String, String] = Map.empty
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[NonEmptySet[TopicPartition]] =
    topics.flatTraverse(createCustomTopic(_, partitions, topicConfig)).map(_.toNes)

  def publishStringMessage(topic: String, key: String, message: String)(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] =
    F.blocking(Underlying.publishToKafka(topic, key, message))

  def publishStringMessages(topic: String, messages: Seq[(String, String)])(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] =
    messages.traverse { case (k, v) => publishStringMessage(topic, k, v) }.void

  def consumeStringMessage(topic: String, autoCommit: Boolean)(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[String] =
    F.blocking(Underlying.consumeFirstStringMessageFrom(topic, autoCommit = autoCommit))

}
