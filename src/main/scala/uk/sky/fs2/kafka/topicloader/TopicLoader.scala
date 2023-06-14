package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap, OptionT}
import cats.effect.Async
import cats.syntax.all.*
import cats.{Monad, Order, Show}
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

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }

  implicit val topicPartitionOrdering: Ordering[TopicPartition] = (x: TopicPartition, y: TopicPartition) =>
    x.hashCode().compareTo(y.hashCode())

  implicit val topicPartitionOrder: Order[TopicPartition] = Order.fromOrdering[TopicPartition]

  private implicit val partitionShow: Show[TopicPartition] = Show.show(tp => s"${tp.topic}-${tp.partition}")

}

trait TopicLoader {

  import TopicLoader.*

  def load[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    assignedConsumerWithOffsets(topics, strategy, consumerSettings).flatMap { case (consumer, logOffsets) =>
      consumer.records
        .map(_.record)
        .through(filterBelowHighestOffset(logOffsets))
    }

  def partitionedLoad[F[_] : Async : LoggerFactory, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] = {
    val logger = LoggerFactory[F].getLogger

    assignedConsumerWithOffsets(topics, strategy, consumerSettings).flatMap { case (consumer, logOffsets) =>
      consumer.partitionsMapStream.flatMap { partitionWithStream =>
        Stream
          .evalSeq(partitionWithStream.toList.traverse { case (partition, inputStream) =>
            logger.warn(s"Assigned ${partition.show}") *> inputStream.pure
          })
      }.parJoinUnbounded
        .map(_.record)
        .through(filterBelowHighestOffset(logOffsets))
    }
  }

  def assignedConsumerWithOffsets[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, (KafkaConsumer[F, K, V], NonEmptyMap[TopicPartition, LogOffsets])] = KafkaConsumer
    .stream(consumerSettings)
    .evalMap { consumer =>
      val maybeConsumerAndOffsets = {
        for {
          allLogOffsets <- OptionT.liftF(logOffsetsForTopics(topics, strategy, consumer))
          logOffsets    <- OptionT.fromOption(NonEmptyMap.fromMap(SortedMap.from(allLogOffsets)))
          _             <- OptionT.liftF(
                             consumer.assign(logOffsets.keys) *>
                               logOffsets.toNel.traverse { case (tp, o) => consumer.seek(tp, o.lowest) }
                           )
        } yield consumer -> logOffsets
      }.value

      maybeConsumerAndOffsets.map(Stream.fromOption[F](_))
    }
    .flatten

  def loadAndRun(): Unit = ()

  private def filterBelowHighestOffset[F[_] : Monad : LoggerFactory, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = {
    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](logOffsets.toSortedMap.map { case (p, o) => p -> (o.highest - 1) })

    _.evalScan(allHighestOffsets)(emitRecordRemovingConsumedPartition[F, K, V])
      .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
      .collect { case WithRecord(r) => r }
  }

  private def logOffsetsForTopics[F[_] : Async, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Map[TopicPartition, LogOffsets]] = for {
    _                           <- consumer.subscribe(topics)
    partitionInfo               <- topics.toList.flatTraverse(consumer.partitionsFor)
    topicPartitions              = partitionInfo.map(pi => new TopicPartition(pi.topic, pi.partition)).toSet
    beginningOffsetPerPartition <- consumer.beginningOffsets(topicPartitions)
    endOffsets                  <- strategy match {
                                     case LoadAll       => consumer.endOffsets(topicPartitions)
                                     case LoadCommitted => earliestOffsets(consumer, beginningOffsetPerPartition)
                                   }
    logOffsets                   = beginningOffsetPerPartition.map { case (partition, offset) =>
                                     partition -> LogOffsets(offset, endOffsets(partition))
                                   }
    _                           <- consumer.unsubscribe
  } yield logOffsets.filter { case (_, o) => o.highest > o.lowest }

  private def earliestOffsets[F[_] : Monad, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Long]] =
    beginningOffsets.toList.traverse { case (p, o) =>
      val committed = consumer.committed(Set(p))
      committed.map(offsets => p -> offsets.get(p).flatMap(Option.apply).fold(o)(_.offset))
    }.map(_.toMap)

  private def emitRecordRemovingConsumedPartition[F[_] : Monad : LoggerFactory, K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): F[HighestOffsetsWithRecord[K, V]] = {
    val logger = LoggerFactory[F].getLogger

    val partitionHighest: Option[Long] = t.partitionOffsets.get(new TopicPartition(r.topic, r.partition))

    val reachedHighest: OptionT[F, TopicPartition] = for {
      offset  <- OptionT.fromOption[F](partitionHighest)
      highest <- OptionT.fromOption[F](if (r.offset >= offset) new TopicPartition(r.topic, r.partition).some else None)
      _       <- OptionT.liftF(logger.warn(s"Finished loading data from ${r.topic}-${r.partition}"))
    } yield highest

    val updatedHighests = reachedHighest.fold(t.partitionOffsets)(highest => t.partitionOffsets - highest)
    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }
    updatedHighests.map(HighestOffsetsWithRecord(_, emittableRecord))
  }

}
