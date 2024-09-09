package utils

import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.syntax.all.*
import cats.effect.{Async, Sync}
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.instances.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import utils.KafkaContainer.KafkaConfig

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

trait EmbeddedKafka[F[_]] {

  private val groupId = UUID.randomUUID().toString

  def createCustomTopic(topic: String, partitions: Int, topicConfig: Map[String, String])(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[NonEmptyList[TopicPartition]] =
    for {
      topic      <- KafkaAdminClient.resource(adminClientSettings).use { adminClient =>
                      for {
                        topic <- F.delay(NewTopic(topic, partitions, 1: Short))
                        _     <- F.delay(topic.configs(topicConfig.asJava))
                        _     <- adminClient.createTopic(topic)
                      } yield topic
                    }
      partitions <- F.fromOption(
                      NonEmptyList.fromList((0 until topic.numPartitions()).toList),
                      IllegalStateException(s"Partitions cannot be < 1 - got $partitions")
                    )
    } yield partitions.map(TopicPartition(topic.name(), _))

  def createCustomTopics(
      topics: NonEmptyList[String],
      partitions: Int = 2,
      topicConfig: Map[String, String] = Map.empty
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[NonEmptySet[TopicPartition]] =
    topics.flatTraverse(createCustomTopic(_, partitions, topicConfig)).map(_.toNes)

  def publishStringMessage(topic: String, key: String, message: String)(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[Unit] = {
    val record = ProducerRecord(topic, key, message)
    publish(record)
  }

  def publishStringMessages(topic: String, messages: Seq[(String, String)])(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[Unit] = {
    val records = messages.map((k, v) => ProducerRecord(topic, k, v))
    publish(records*)
  }

  def consumeStringMessage(topic: String, autoCommit: Boolean)(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[String] =
    KafkaConsumer.resource(consumerSettings(autoCommit)).use { consumer =>
      for {
        _       <- consumer.subscribeTo(topic)
        message <- consumer.records
                     .take(1)
                     .compile
                     .onlyOrError
                     .timeoutTo(
                       30.seconds,
                       KafkaTimeoutException("Could not consume 1 message within 30 seconds").raiseError
                     )
      } yield message.record.value
    }

  private def consumerSettings(
      autoCommit: Boolean
  )(using kafkaConfig: KafkaConfig, F: Sync[F]): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
      .withEnableAutoCommit(autoCommit)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def producerSettings(using kafkaConfig: KafkaConfig, F: Sync[F]): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

  private def adminClientSettings(using kafkaConfig: KafkaConfig): AdminClientSettings =
    AdminClientSettings(s"localhost:${kafkaConfig.kafkaPort}")

  private def publish(
      pr: ProducerRecord[String, String]*
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[Unit] =
    KafkaProducer.resource(producerSettings).use { producer =>
      producer.produce(ProducerRecords(pr.toList)).flatten.flatTap(pr => F.delay(println(s"got result: $pr"))).void
    }

}
