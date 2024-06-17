package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import cats.{Monad, Show}
import fs2.kafka.instances.*
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.LoggerFactory

import scala.collection.immutable.SortedMap

object TopicLoader extends TopicLoader {
  private[topicloader] case class LogOffsets(lowest: Long, highest: Long)

  private[topicloader] given Show[LogOffsets] =
    Show.show(lo => s"LogOffset(highest=${lo.highest}, lowest=${lo.lowest})")

  private case class PartitionLastOffset(topicPartition: TopicPartition, offset: Long)

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]],
      partitionLastOffset: Option[PartitionLastOffset] = none[PartitionLastOffset]
  )

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }
}

trait TopicLoader {

  import TopicLoader.*

  /** Stream that loads the specified topics from the beginning and completes when the offsets reach the point specified
    * by the requested strategy.
    *
    * @param topics
    *   topics to load
    * @param strategy
    *   A [[LoadTopicStrategy]]
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    */
  def load[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .flatMap(load(topics, strategy, _))

  /** Stream that loads the specified topics from the beginning. When the latest current offsets are reached, the
    * `onLoad` callback is evaluated, and the stream continues.
    *
    * @param topics
    *   topics to load
    * @param consumerSettings
    *   [[fs2.kafka.ConsumerSettings]] for the given topics
    * @param onLoad
    *   A callback that will be evaluated on once the current offsets are reached
    */
  def loadAndRun[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      consumerSettings: ConsumerSettings[F, K, V]
  )(onLoad: Resource.ExitCase => F[Unit]): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .flatMap { consumer =>
        load(topics, LoadAll, consumer).onFinalizeCase(onLoad) ++ consumer.records.map(_.record)
      }

  private def load[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    for {
      logOffsets <- Stream.eval(logOffsetsForTopics(topics, strategy, consumer)).flatMap(Stream.fromOption(_))
      _          <- Stream.eval(assignOffsets(logOffsets, consumer))
      record     <- consumer.records.map(_.record).through(filterBelowHighestOffset(logOffsets))
    } yield record

  private def assignOffsets[F[_] : Monad : LoggerFactory, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  ): F[Unit] = {
    val logger = LoggerFactory[F].getLogger
    for {
      _ <- logger.debug(s"Assigning partitions: ${logOffsets.keys.mkString_(",")}")
      _ <- consumer.assign(logOffsets.keys)
      _ <- logOffsets.toNel.traverse { (tp, o) =>
             logger.debug(s"Seeking to offset ${o.lowest} for partition ${tp.show}") *>
               consumer.seek(tp, o.lowest)
           }
    } yield ()
  }

  private def filterBelowHighestOffset[F[_] : Monad : LoggerFactory, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = stream => {
    val logger = LoggerFactory[F].getLogger

    val (nonEmptyOffsets, emptyOffsets) =
      logOffsets.toSortedMap.partition((_, o) => o.highest > o.lowest)

    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](nonEmptyOffsets.map((p, o) => p -> (o.highest - 1)))

    Stream.eval {
      for {
        _ <- logger.debug(s"Non-empty offsets: ${nonEmptyOffsets.toList.mkString_(", ")}")
        _ <- logger.debug(s"Empty offsets: ${emptyOffsets.toList.mkString_(", ")}")
        _ <- emptyOffsets.toList.traverse { (tp, o) =>
               logger.info(
                 s"Not loading data from ${tp.show} as lowest offset ${o.lowest} <= highest offset ${o.highest}"
               )
             }
      } yield ()
    } >>
      stream
        .scan(allHighestOffsets)(emitRecordRemovingConsumedPartition[K, V])
        .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
        .evalTapChunk(_.partitionLastOffset.traverse { last =>
          logger.info(s"Finished loading data from ${last.topicPartition.show} at offset ${last.offset}")
        })
        .collect { case WithRecord(r) => r }
  }

  private def logOffsetsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Option[NonEmptyMap[TopicPartition, LogOffsets]]] =
    for {
      topicPartitions             <- topics.toList.flatTraverse { topic =>
                                       // Assign doesn't support incremental subscription, so we must aggregate partitions per topic
                                       consumer.assign(topic) *> partitionsForTopics(topics, consumer).map(_.toList)
                                     }
                                       .map(_.toSet)
      beginningOffsetPerPartition <- consumer.beginningOffsets(topicPartitions)
      endOffsets                  <- strategy match {
                                       case LoadAll       => consumer.endOffsets(topicPartitions)
                                       case LoadCommitted => earliestOffsets(consumer, beginningOffsetPerPartition)
                                     }
      logOffsets                   = beginningOffsetPerPartition.map { (partition, offset) =>
                                       partition -> LogOffsets(offset, endOffsets(partition))
                                     }
      _                           <- consumer.unsubscribe
    } yield NonEmptyMap.fromMap(SortedMap.from(logOffsets))

  private def earliestOffsets[F[_] : Monad, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Long]] =
    beginningOffsets.toList.traverse { (p, o) =>
      val committed = consumer.committed(Set(p))
      committed.map(offsets => p -> offsets.get(p).flatMap(Option.apply).fold(o)(_.offset))
    }.map(_.toMap)

  private def partitionsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      consumer: KafkaConsumer[F, K, V]
  ): F[Set[TopicPartition]] =
    for {
      partitionInfo <- topics.toList.flatTraverse(consumer.partitionsFor)
    } yield partitionInfo.map(pi => TopicPartition(pi.topic, pi.partition)).toSet

  private def emitRecordRemovingConsumedPartition[K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): HighestOffsetsWithRecord[K, V] = {
    val partitionHighest: Option[Long] = t.partitionOffsets.get(TopicPartition(r.topic, r.partition))

    val reachedHighest: Option[TopicPartition] = for {
      offset  <- partitionHighest
      highest <- Option.when(r.offset >= offset)(TopicPartition(r.topic, r.partition))
    } yield highest

    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }

    reachedHighest match {
      case Some(highest) =>
        HighestOffsetsWithRecord(
          partitionOffsets = t.partitionOffsets - highest,
          consumerRecord = emittableRecord,
          partitionLastOffset = PartitionLastOffset(highest, r.offset).some
        )

      case None =>
        HighestOffsetsWithRecord(
          partitionOffsets = t.partitionOffsets,
          consumerRecord = emittableRecord,
          partitionLastOffset = none[PartitionLastOffset]
        )
    }
  }

}
