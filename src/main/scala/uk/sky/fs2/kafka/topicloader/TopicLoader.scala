package uk.sky.fs2.kafka.topicloader

import cats.Monad
import cats.data.{NonEmptyList, NonEmptyMap, OptionT}
import cats.effect.Async
import cats.syntax.all.*
import fs2.kafka.{ConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.Logger

import scala.collection.immutable.SortedMap

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

trait TopicLoader {

  import TopicLoader.*

  def load[F[_] : Async : Logger, K, V](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, K, V]
  ): Stream[F, ConsumerRecord[K, V]] =
    KafkaConsumer
      .stream(consumerSettings)
      .evalMap(consumer =>
        for {
          logOffsets     <- logOffsetsForTopics(topics, strategy, consumer)
          maybeLogOffsets = NonEmptyMap.fromMap(SortedMap.from(logOffsets))
        } yield maybeLogOffsets.fold[Stream[F, ConsumerRecord[K, V]]](Stream.empty) { logOffsets =>
          Stream
            .eval(
              for {
                _ <- consumer.assign(logOffsets.keys)
                _ <- logOffsets.toNel.traverse { case (tp, o) => consumer.seek(tp, o.lowest) }
              } yield consumer.records
                .map(_.record)
                .through(filterBelowHighestOffset(logOffsets))
            )
            .flatten
        }
      )
      .flatten

  def loadAndRun(): Unit = ()

  protected def filterBelowHighestOffset[F[_] : Monad : Logger, K, V](
      logOffsets: NonEmptyMap[TopicPartition, LogOffsets]
  ): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] = {
    val allHighestOffsets: HighestOffsetsWithRecord[K, V] =
      HighestOffsetsWithRecord[K, V](logOffsets.toSortedMap.map { case (p, o) => p -> (o.highest - 1) })

    _.evalScan(allHighestOffsets)(emitRecordRemovingConsumedPartition[F, K, V])
      .takeWhile(_.partitionOffsets.nonEmpty, takeFailure = true)
      .collect { case WithRecord(r) => r }
  }

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
      partitionInfo    <- topics.toList.flatTraverse(consumer.partitionsFor)
      partitions        = partitionInfo.map(pi => new TopicPartition(pi.topic, pi.partition)).toSet
      beginningOffsets <- consumer.beginningOffsets(partitions)
      endOffsets       <- strategy match {
                            case LoadAll       => consumer.endOffsets(partitions)
                            case LoadCommitted => earliestOffsets(consumer, beginningOffsets)
                          }
      logOffsets        = beginningOffsets.map { case (k, v) => k -> LogOffsets(v, endOffsets(k)) }
      _                <- consumer.unsubscribe
    } yield logOffsets.filter { case (_, o) => o.highest > o.lowest }
  }

  private def emitRecordRemovingConsumedPartition[F[_] : Monad, K, V](
      t: HighestOffsetsWithRecord[K, V],
      r: ConsumerRecord[K, V]
  )(implicit logger: Logger[F]): F[HighestOffsetsWithRecord[K, V]] = {
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
