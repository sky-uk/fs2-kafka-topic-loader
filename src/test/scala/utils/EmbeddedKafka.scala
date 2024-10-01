package utils

import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.instances.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

trait EmbeddedKafka[F[_]] {

  private val groupId = UUID.randomUUID().toString

  def withRunningKafka[T](test: KafkaConfig => F[T])(using Async[F]): F[T] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    KafkaConfig
      .random[F]
      .flatMap(kafkaConfig => KafkaServer.resource[F](kafkaConfig).surround(test(kafkaConfig)))
  }

  def createCustomTopic(topic: String, partitions: Int, topicConfig: Map[String, String])(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[NonEmptyList[TopicPartition]] =
    for {
      newTopic   <- F.delay {
                      val newTopic = NewTopic(topic, partitions, 1: Short)
                      newTopic.configs(topicConfig.asJava)
                      newTopic
                    }
      _          <- withAdminClient(_.createTopic(newTopic))
      partitions <- NonEmptyList
                      .fromList((0 until newTopic.numPartitions()).toList)
                      .liftTo[F](IllegalStateException(s"Partitions cannot be < 1 - got $partitions"))
    } yield partitions.map(TopicPartition(newTopic.name(), _))

  def createCustomTopics(
      topics: NonEmptyList[String],
      partitions: Int = 2,
      topicConfig: Map[String, String] = Map.empty
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[NonEmptySet[TopicPartition]] =
    topics.flatTraverse(createCustomTopic(_, partitions, topicConfig)).map(_.toNes)

  def publishStringMessage(topic: String, key: String, message: String)(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[Unit] = publishStringMessages(topic, Seq(key -> message))

  def publishStringMessages(topic: String, messages: Seq[(String, String)])(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[Unit] = {
    val records = messages.map((k, v) => ProducerRecord(topic, k, v))
    withProducer(_.produce(ProducerRecords(records)).flatten).void
  }

  def consumeStringMessage(topic: String, autoCommit: Boolean)(using
      kafkaConfig: KafkaConfig,
      F: Async[F]
  ): F[String] =
    withConsumer(autoCommit) { consumer =>
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

  def withConsumer[T](
      autoCommit: Boolean,
      autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Earliest,
      groupId: String = groupId
  )(f: KafkaConsumer[F, String, String] => F[T])(using kafkaConfig: KafkaConfig, F: Async[F]): F[T] = {
    val consumerSettings = ConsumerSettings[F, String, String]
      .withBootstrapServers(kafkaConfig.plaintextListener)
      .withEnableAutoCommit(autoCommit)
      .withAutoOffsetReset(autoOffsetReset)
      .withGroupId(groupId)

    KafkaConsumer.resource(consumerSettings).use(f)
  }

  def withProducer[T](
      f: KafkaProducer.PartitionsFor[F, String, String] => F[T]
  )(using kafkaConfig: KafkaConfig, F: Async[F]): F[T] = {
    val producerSettings = ProducerSettings[F, String, String]
      .withBootstrapServers(kafkaConfig.plaintextListener)

    KafkaProducer.resource(producerSettings).use(f)
  }

  def withAdminClient[T](f: KafkaAdminClient[F] => F[T])(using kafkaConfig: KafkaConfig, F: Async[F]): F[T] = {
    val adminClientSettings = AdminClientSettings(kafkaConfig.plaintextListener)

    KafkaAdminClient.resource(adminClientSettings).use(f)
  }
}
