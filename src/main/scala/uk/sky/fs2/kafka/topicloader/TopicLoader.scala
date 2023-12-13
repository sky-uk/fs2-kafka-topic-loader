package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap, OptionT}
import cats.effect.Async
import cats.effect.kernel.Resource
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

  private[topicloader] given [K, V]: Show[ConsumerRecord[K, V]] =
    Show.show(cr => s"${cr.topic}-${cr.partition}")

  private case class HighestOffsetsWithRecord[K, V](
      partitionOffsets: Map[TopicPartition, Long],
      consumerRecord: Option[ConsumerRecord[K, V]] = none[ConsumerRecord[K, V]]
  )

  private object WithRecord {
    def unapply[K, V](h: HighestOffsetsWithRecord[K, V]): Option[ConsumerRecord[K, V]] = h.consumerRecord
  }

}

trait TopicLoader {

  import TopicLoader.{*, given}

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
  ): Stream[F, ConsumerRecord[K, V]] = Stream.eval {
    val logger = LoggerFactory[F].getLogger
    {
      for {
        logOffsets <- OptionT(logOffsetsForTopics(topics, strategy, consumer))
        _          <- OptionT.liftF(logger.debug(s"Log Offsets for topics: ${logOffsets.show}"))
        _          <- OptionT.liftF(logger.debug(s"Assigning partitions: ${logOffsets.keys.mkString_(",")}"))
        _          <- OptionT.liftF(consumer.assign(logOffsets.keys))
        _          <- OptionT.liftF(logOffsets.toNel.traverse { (tp, o) =>
                        logger.debug(s"Seeking to offset ${o.lowest} for partition ${tp.show}") *>
                          consumer.seek(tp, o.lowest)
                      })
      } yield consumer.records.map(_.record).through(filterBelowHighestOffset(logOffsets))
    }.getOrElse(Stream.empty)
  }.flatten

  private def filterBelowHighestOffset[F[_] : Monad : LoggerFactory, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = {
    val nonEmptyOffsets =
      logOffsets.toSortedMap.filter((_, o) => o.highest > 0)

    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](nonEmptyOffsets.map((p, o) => p -> (o.highest - 1)))

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

  private def emitRecordRemovingConsumedPartition[F[_] : Monad : LoggerFactory, K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  ): F[HighestOffsetsWithRecord[K, V]] = {
    val logger = LoggerFactory[F].getLogger

    val partitionHighest: Option[Long] = t.partitionOffsets.get(TopicPartition(r.topic, r.partition))

    val reachedHighest: OptionT[F, TopicPartition] = for {
      offset  <- OptionT.fromOption[F](partitionHighest)
      highest <- OptionT.fromOption[F](if (r.offset >= offset) TopicPartition(r.topic, r.partition).some else None)
      _       <- OptionT.liftF(logger.warn(s"Finished loading data from ${r.show} at offset ${r.offset}"))
    } yield highest

    val updatedHighests = reachedHighest.fold(t.partitionOffsets)(highest => t.partitionOffsets - highest)
    val emittableRecord = partitionHighest.collect { case h if r.offset <= h => r }
    updatedHighests.map(HighestOffsetsWithRecord(_, emittableRecord))
  }

}
