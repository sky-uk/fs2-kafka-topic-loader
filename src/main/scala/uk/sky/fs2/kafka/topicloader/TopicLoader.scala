package uk.sky.fs2.kafka.topicloader

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.scalalogging.LazyLogging
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer, KafkaDeserializer}
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

object TopicLoader extends TopicLoader {
  private[topicloader] case class LogOffsets(lowest: Long, highest: Long)

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]]
  )

  private implicit class DeserializerOps(private val bytes: Array[Byte]) extends AnyVal {
    def deserialize[T](topic: String)(implicit ds: KafkaDeserializer[T]): T = ds.deserialize(topic, bytes)
  }

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }
}

trait TopicLoader extends LazyLogging {

  import TopicLoader.*

  def load[F[_] : Async, K : Deserializer, V : Deserializer](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
  ): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalMap { consumer =>
        for {
          logOffsets <- logOffsetsForTopics(topics, strategy, consumer)
          _           = logger.warn(s"Subscribed to ${topics.mkString_(", ")}")
          _          <- consumer.seekToBeginning
        } yield {
          val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
            HighestOffsetsWithRecord[K, V](logOffsets.map { case (p, o) => p -> (o.highest - 1) })

          val filterBelowHighestOffset: Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] =
            _.scan(allHighestOffsets)(emitRecordRemovingConsumedPartition)
              .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
              .collect { case WithRecord(r) => r }

          consumer.records
            .map(cr => cr.bimap(_.deserialize[K](cr.record.topic), _.deserialize[V](cr.record.topic)).record)
            .through(filterBelowHighestOffset)
        }
      }
      .flatten

  def loadAndRun(): Unit = ()

  protected def logOffsetsForTopics[F[_] : Async](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, Array[Byte], Array[Byte]]
  ): F[Map[TopicPartition, LogOffsets]] = {
    def earliestOffsets(
        consumer: KafkaConsumer[F, Array[Byte], Array[Byte]],
        beginningOffsets: Map[TopicPartition, Long]
    ): F[Map[TopicPartition, Long]] =
      beginningOffsets.toList.traverse { case (p, o) =>
        println(s"Partition: ${p}")
        println(s"Offsets: ${o}")
        val committed = consumer.committed(Set(p))
        println(s"committed: $committed")
        committed.map { foo =>
          println(s"committed 2: ${foo}")
          val toGet = foo.get(p).flatMap(Option.apply)
          println(s"toGet: ${toGet}")
          p -> toGet.fold(o)(_.offset)
        }
      }.map(_.toMap)

    for {
      _                <- consumer.subscribeTo(topics.head, topics.tail*)
      partitionInfo    <- topics.toList.flatTraverse(foo => consumer.partitionsFor(foo))
      partitions        = partitionInfo.map(pi => new TopicPartition(pi.topic, pi.partition)).toSet
      beginningOffsets <- consumer.beginningOffsets(partitions)
      _                 = println(s"beginningOffsets: $beginningOffsets")
      endOffsets       <- strategy match {
                            case LoadAll       => consumer.endOffsets(partitions)
                            case LoadCommitted => earliestOffsets(consumer, beginningOffsets)
                          }
      _                 = println(s"endOffsets: $endOffsets")
      logOffsets        = beginningOffsets.map { case (k, v) => k -> LogOffsets(v, endOffsets(k)) }
    } yield logOffsets.filter { case (_, o) => o.highest > o.lowest }
  }

  private def emitRecordRemovingConsumedPartition[K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): HighestOffsetsWithRecord[K, V] = {
    val partitionHighest: Option[Long] = t.partitionOffsets.get(new TopicPartition(r.topic, r.partition))

    val reachedHighest: Option[TopicPartition] = for {
      offset  <- partitionHighest
      highest <- if (r.offset >= offset) new TopicPartition(r.topic, r.partition).some else None
      _        = logger.warn(s"Finished loading data from ${r.topic}-${r.partition}")
    } yield highest

    val updatedHighests = reachedHighest.fold(t.partitionOffsets)(highest => t.partitionOffsets - highest)
    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }
    HighestOffsetsWithRecord(updatedHighests, emittableRecord)
  }

}
