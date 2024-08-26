package utils

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.instances.*
import io.github.embeddedkafka.{EmbeddedKafka as Underlying, EmbeddedKafkaConfig}
import kafka.server.KafkaServer
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters.*

// TODO - completely remove embedded kafka and use FS2 Kafka
trait EmbeddedKafka[F[_]] {

  // TODO - remove and change int tests
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
      tpIndexes <- F.fromOption(
                     NonEmptyList.fromList((0 until partitions).toList),
                     IllegalStateException(s"Partitions cannot be < 1 - got $partitions")
                   )
      topic     <- KafkaAdminClient.resource(adminClientSettings).use { adminClient =>
                     for {
                       topic <- F.delay(NewTopic(topic, partitions, 1: Short))
                       _     <- F.delay(topic.configs(topicConfig.asJava))
                       _     <- adminClient.createTopic(topic)
                     } yield topic
                   }
    } yield tpIndexes.map(TopicPartition(topic.name(), _))

  def createCustomTopics(
      topics: NonEmptyList[String],
      partitions: Int = 2,
      topicConfig: Map[String, String] = Map.empty
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[NonEmptySet[TopicPartition]] =
    topics.flatTraverse(createCustomTopic(_, partitions, topicConfig)).map(_.toNes)

  private def publish(
      pr: ProducerRecord[String, String]*
  )(using kafkaConfig: EmbeddedKafkaConfig, F: Async[F]): F[Unit] =
    KafkaProducer.resource(producerSettings).use { producer =>
      producer.produce(ProducerRecords(pr.toList)).flatten.void
    }

  def publishStringMessage(topic: String, key: String, message: String)(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] = {
    val record = ProducerRecord(topic, key, message)
    publish(record)
  }

  def publishStringMessages(topic: String, messages: Seq[(String, String)])(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[Unit] = {
    val records = messages.map((k, v) => ProducerRecord(topic, k, v))
    publish(records*)
  }

  def consumeStringMessage(topic: String, autoCommit: Boolean)(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Async[F]
  ): F[String] =
    F.blocking(Underlying.consumeFirstStringMessageFrom(topic, autoCommit = autoCommit))

  private def producerSettings(using
      kafkaConfig: EmbeddedKafkaConfig,
      F: Sync[F]
  ): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

  private def adminClientSettings(using kafkaConfig: EmbeddedKafkaConfig): AdminClientSettings =
    AdminClientSettings(s"localhost:${kafkaConfig.kafkaPort}")

}
