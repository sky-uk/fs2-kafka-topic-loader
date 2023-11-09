package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap, OptionT}
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.syntax.all.*
import cats.{Monad, Order}
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.LoggerFactory

import scala.collection.immutable.SortedMap

object TopicLoader extends TopicLoader {
  private[topicloader] case class LogOffsets(lowest: Long, highest: Long)

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]]
  )

  given topicPartitionOrdering: Ordering[TopicPartition] = (x: TopicPartition, y: TopicPartition) =>
    x.hashCode().compareTo(y.hashCode())

  given topicPartitionOrder: Order[TopicPartition] = Order.fromOrdering[TopicPartition]

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }
}

trait TopicLoader {

  import TopicLoader.{*, given}

  def load[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalMap(consumer => OptionT(loadIfNonEmpty(topics, strategy, consumer)).getOrElse(Stream.empty))
      .flatten

  def loadAndRun[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      consumerSettings: ConsumerSettings[F, K, V]
  )(onLoad: Resource.ExitCase => F[Unit]): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalMap { consumer =>
        OptionT(loadIfNonEmpty(topics, LoadAll, consumer))
          .getOrElse(
            // If there are no logOffsets we need to ensure we're subscribed to continue streaming
            Stream.eval(consumer.subscribe(topics)).drain
          )
          .map(_.onFinalizeCase(onLoad) ++ consumer.records.map(_.record))
      }
      .flatten

  private def loadIfNonEmpty[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Option[Stream[F, ConsumerRecord[K, V]]]] = {
    for {
      logOffsets <- OptionT(logOffsetsForTopics(topics, strategy, consumer))
      _          <- OptionT.liftF(consumer.assign(logOffsets.keys))
      _          <- OptionT.liftF(logOffsets.toNel.traverse { case (tp, o) => consumer.seek(tp, o.lowest) })
    } yield consumer.records.map(_.record).through(filterBelowHighestOffset(logOffsets))
  }.value

  private def filterBelowHighestOffset[F[_] : Async : LoggerFactory, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = {
    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](logOffsets.toSortedMap.map((p, o) => p -> (o.highest - 1)))

    _.evalScan(allHighestOffsets)(emitRecordRemovingConsumedPartition[F, K, V])
      .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
      .collect { case WithRecord(r) => r }
  }

  private def logOffsetsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Option[NonEmptyMap[TopicPartition, LogOffsets]]] =
    for {
      topicPartitions             <-
        topics.toList
          .flatTraverse(topic =>
            // Assign doesn't support incremental subscription, so we must aggregate partitions per topic
            consumer.assign(topic) *> partitionsForTopics(topics, consumer).map(_.toList)
          )
          .map(_.toSet)
      beginningOffsetPerPartition <- consumer.beginningOffsets(topicPartitions)
      endOffsets                  <- strategy match {
                                       case LoadAll       => consumer.endOffsets(topicPartitions)
                                       case LoadCommitted => earliestOffsets(consumer, beginningOffsetPerPartition)
                                     }
      logOffsets                   = beginningOffsetPerPartition.map { case (partition, offset) =>
                                       partition -> LogOffsets(offset, endOffsets(partition))
                                     }
      _                           <- consumer.unsubscribe
    } yield {
      val offsets = logOffsets.filter { case (_, o) => o.highest > o.lowest }
      NonEmptyMap.fromMap(SortedMap.from(offsets))
    }

  private def earliestOffsets[F[_] : Monad, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Long]] =
    beginningOffsets.toList.traverse { case (p, o) =>
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

  private def emitRecordRemovingConsumedPartition[F[_] : Monad : LoggerFactory, K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): F[HighestOffsetsWithRecord[K, V]] = {
    val logger = LoggerFactory[F].getLogger

    val partitionHighest: Option[Long] = t.partitionOffsets.get(TopicPartition(r.topic, r.partition))

    val reachedHighest: OptionT[F, TopicPartition] = for {
      offset  <- OptionT.fromOption[F](partitionHighest)
//      _       <- OptionT.liftF(logger.warn(s"offset: $offset"))
      highest <- OptionT.fromOption[F](if (r.offset >= offset) TopicPartition(r.topic, r.partition).some else None)
//      _       <- OptionT.liftF(logger.warn(s"highest: $highest"))
      _       <- OptionT.liftF(logger.warn(s"Finished loading data from ${r.topic}-${r.partition} at offset ${r.offset}"))
    } yield highest

    val updatedHighests = reachedHighest.fold(t.partitionOffsets)(highest => t.partitionOffsets - highest)
    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }
    updatedHighests.map(HighestOffsetsWithRecord(_, emittableRecord))
  }

}
