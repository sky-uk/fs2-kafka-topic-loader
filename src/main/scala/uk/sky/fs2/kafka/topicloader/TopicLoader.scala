package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.scalalogging.LazyLogging
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.SortedSet

object TopicLoader extends TopicLoader {
  private[topicloader] case class LogOffsets(lowest: Long, highest: Long)

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]]
  )

  private implicit object TopicPartitionOrdering extends Ordering[TopicPartition] {
    override def compare(x: TopicPartition, y: TopicPartition): Int = x.hashCode().compareTo(y.hashCode())
  }

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }
}

trait TopicLoader extends LazyLogging {

  import TopicLoader.*

  def load[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalMap { consumer =>
        for {
          logOffsets     <- logOffsetsForTopics(topics, strategy, consumer)
          maybePartitions = NonEmptySet.fromSet(SortedSet.from(logOffsets.keySet))
          stream         <-
            maybePartitions.fold[F[Stream[F, ConsumerRecord[K, V]]]](Async[F].pure(Stream.empty)) { partitions =>
              logger.debug(s"non empty partitions: $partitions")
              for {
                _ <- consumer.assign(partitions)
                _ <- logOffsets.toList.traverse { case (tp, o) => consumer.seek(tp, o.lowest) }
              } yield {

                val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
                  HighestOffsetsWithRecord[K, V](logOffsets.map { case (p, o) => p -> (o.highest - 1) })

                logger.debug(s"allHighestOffsets: $allHighestOffsets")

                val filterBelowHighestOffset: Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] =
                  _.scan(allHighestOffsets)(emitRecordRemovingConsumedPartition)
                    .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
                    .collect { case WithRecord(r) => r }

                consumer.records
                  .map(_.record)
                  .through(filterBelowHighestOffset)
              }
            }
        } yield stream
      }
      .flatten

  def loadAndRun(): Unit = ()

  protected def logOffsetsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Map[TopicPartition, LogOffsets]] = {
    def earliestOffsets(
        consumer: KafkaConsumer[F, K, V],
        beginningOffsets: Map[TopicPartition, Long]
    ): F[Map[TopicPartition, Long]] =
      beginningOffsets.toList.traverse { case (p, o) =>
        val committed = consumer.committed(Set(p))
        committed.map(offsets => p -> offsets.get(p).flatMap(Option.apply).fold(o)(_.offset))
      }.map(_.toMap)

    for {
      _                <- consumer.subscribe(topics)
      _                 = logger.debug(s"Subscribed to ${topics.mkString_(", ")}")
      partitionInfo    <- topics.toList.flatTraverse(consumer.partitionsFor)
      partitions        = partitionInfo.map(pi => new TopicPartition(pi.topic, pi.partition)).toSet
      beginningOffsets <- consumer.beginningOffsets(partitions)
      _                 = logger.debug(s"beginningOffsets: $beginningOffsets")
      endOffsets       <- strategy match {
                            case LoadAll       => consumer.endOffsets(partitions)
                            case LoadCommitted => earliestOffsets(consumer, beginningOffsets)
                          }
      _                 = logger.debug(s"endOffsets: $endOffsets")
      logOffsets        = beginningOffsets.map { case (k, v) => k -> LogOffsets(v, endOffsets(k)) }
      _                 = logger.debug(s"logOffsets: $logOffsets")
      _                <- consumer.unsubscribe
    } yield {
      val nonEmptyOffsets = logOffsets.filter { case (_, o) => o.highest > o.lowest }
      logger.debug(s"nonEmptyOffsets: $nonEmptyOffsets")
      nonEmptyOffsets
    }
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
