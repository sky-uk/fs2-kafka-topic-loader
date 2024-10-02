package uk.sky.fs2.kafka.topicloader

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import cats.{Monad, Show}
import fs2.kafka.instances.*
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.syntax.*
import org.typelevel.log4cats.{Logger, LoggerFactory}

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
  ): Stream[F, ConsumerRecord[K, V]] = {
    given Logger[F] = LoggerFactory[F].getLogger
    KafkaConsumer
      .stream(consumerSettings)
      .flatMap(load(topics, strategy, _, "foo"))
  }

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
  )(onLoad: Resource.ExitCase => F[Unit]): Stream[F, ConsumerRecord[K, V]] = {
    given Logger[F] = LoggerFactory[F].getLogger

    def postLoad(logOffsets: NonEmptyMap[TopicPartition, LogOffsets]): Stream[F, ConsumerRecord[K, V]] =
      for {
        // The only consistent workaround for re-assigning offsets after the initial load is to re-create the consumer
        postLoadConsumer <- KafkaConsumer.stream(consumerSettings)
        _                <- Stream.eval(assignOffsets(logOffsets, postLoadConsumer)(_.highest))
        record           <- postLoadConsumer.records.map(_.record)
      } yield record

    for {
      preloadConsumer <- KafkaConsumer.stream(consumerSettings)
      logOffsets      <- Stream.eval(logOffsetsForTopics(topics, LoadAll, preloadConsumer)).flatMap(Stream.fromOption(_))
      _               <- Stream.eval(info"log offsets: ${logOffsets.show}")
      record          <- load(logOffsets, preloadConsumer).onFinalizeCase(onLoad) ++ postLoad(logOffsets)
    } yield record
  }

  private def load[F[_] : Async : Logger, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V],
      foo: String
  ): Stream[F, ConsumerRecord[K, V]] = {
    println(foo)
    for {
      logOffsets <- Stream.eval(logOffsetsForTopics(topics, strategy, consumer)).flatMap(Stream.fromOption(_))
      _          <- Stream.eval(info"log offsets: ${logOffsets.show}")
      record     <- load(logOffsets, consumer)
    } yield record
  }

  private def load[F[_] : Async : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    for {
      _      <- Stream.eval(assignOffsets(logOffsets, consumer)(_.lowest))
      record <- consumer.records.map(_.record).through(filterBelowHighestOffset(logOffsets))
    } yield record

  private def assignOffsets[F[_] : Monad : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets],
      consumer: KafkaConsumer[F, K, V]
  )(position: LogOffsets => Long): F[Unit] =
    for {
      _ <- debug"Assigning partitions: ${logOffsets.keys.mkString_(",")}"
      _ <- consumer.assign(logOffsets.keys)
      _ <- logOffsets.toNel.traverse_ { (tp, o) =>
             debug"Seeking to offset ${position(o)} for partition ${tp.show}" >> consumer.seek(tp, position(o))
           }
    } yield ()

  private def filterBelowHighestOffset[F[_] : Monad : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = stream => {
    val (nonEmptyOffsets, emptyOffsets) =
      logOffsets.toSortedMap.partition((_, o) => o.highest > o.lowest)

    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](nonEmptyOffsets.map((p, o) => p -> (o.highest - 1)))

    Stream.eval {
      emptyOffsets.toList.traverse((tp, o) => info"Not loading data from ${tp.show} at offset ${o.highest}")
    } >>
      stream
        .scan(allHighestOffsets)(emitRecordRemovingConsumedPartition[K, V])
        .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
        .evalTapChunk(_.partitionLastOffset.traverse { last =>
          info"Finished loading data from ${last.topicPartition.show} at offset ${last.offset}"
        })
        .collect { case WithRecord(r) => r }
  }

  private def logOffsetsForTopics[F[_] : Async : Logger, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumer: KafkaConsumer[F, K, V]
  ): F[Option[NonEmptyMap[TopicPartition, LogOffsets]]] =
    for {
      topicPartitions             <- partitionsForTopics(topics, consumer)
      beginningOffsetPerPartition <- consumer.beginningOffsets(topicPartitions)
      endOffsets                  <- strategy match {
                                       case LoadAll       => consumer.endOffsets(topicPartitions)
                                       case LoadCommitted => earliestOffsets(consumer, beginningOffsetPerPartition)
                                     }
      logOffsets                   = beginningOffsetPerPartition.map { (partition, offset) =>
                                       partition -> LogOffsets(offset, endOffsets(partition))
                                     }
    } yield NonEmptyMap.fromMap(SortedMap.from(logOffsets))

  private def earliestOffsets[F[_] : Monad : Logger, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Long]] =
    for {
      committed       <- offsetsAndMetadataFor(consumer, beginningOffsets)
      earliestOffsets <- beginningOffsets.toList.traverse { (tp, beginningOffset) =>
                           for {
                             maybeCommitted <- committed.get(tp).pure
                             earliest       <- maybeCommitted.fold(beginningOffset)(_.offset).pure
                             _              <- debug"Earliest offset for ${tp.show}: $earliest"
                           } yield tp -> earliest
                         }
    } yield earliestOffsets.toMap

  private def offsetsAndMetadataFor[F[_] : Monad : Logger, K, V](
      consumer: KafkaConsumer[F, K, V],
      beginningOffsets: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndMetadata]] =
    for {
      committed       <- consumer.committed(beginningOffsets.keySet)
      filteredOffsets <- committed.toList.traverseFilter { (tp, offsetAndMetadata) =>
                           // It is possible to return null for an empty topic
                           Option(offsetAndMetadata) match {
                             case Some(offsetAndMetadata) =>
                               debug"${tp.show} had committed offset: ${offsetAndMetadata.show}"
                                 .as(Some(tp -> offsetAndMetadata))

                             case None =>
                               debug"${tp.show} had no committed offset"
                                 .as(none[(TopicPartition, OffsetAndMetadata)])
                           }
                         }
    } yield filteredOffsets.toMap

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
