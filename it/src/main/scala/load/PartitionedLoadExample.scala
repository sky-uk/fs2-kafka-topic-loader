package load

import cats.data.NonEmptyList
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import cats.{Show, Traverse}
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.apache.kafka.common.TopicPartition
import org.typelevel.log4cats.LoggerFactory
import uk.sky.fs2.kafka.topicloader.{LoadCommitted, TopicLoader}

/** Simple application that streams Kafka records into a memory store, and produce the same record back to Kafka. On
  * startup, it will read all of the currently committed records back into this store, but not produce them to Kafka.
  */
class PartitionedLoadExample[F[_] : Async : LoggerFactory, G[_] : Traverse](
    load: Stream[F, String],
    run: Stream[F, G[String]],
    publishAndCommit: Pipe[F, G[String], Nothing],
    store: Ref[F, List[String]]
) {
  private val logger = LoggerFactory[F].getLogger

  private val process = (message: String) => store.update(_ :+ message)

  private val clearStore = store.update(_.empty)

  val stream: Stream[F, G[String]] =
    (load.evalTap(process).drain ++ run.evalTap(_.traverse(process))).observe(publishAndCommit).onFinalizeCase {
      case ExitCase.Succeeded  => logger.warn("Stream terminated - Success") *> clearStore
      case ExitCase.Errored(e) => logger.warn(s"Stream terminated - Error: ${e.getMessage}") *> clearStore
      case ExitCase.Canceled   => logger.warn("Stream terminated - Canceled") *> clearStore
    }

}

object PartitionedLoadExample {
  def partitionedKafka[F[_] : Async : LoggerFactory](
      topics: NonEmptyList[String],
      outputTopic: String,
      consumerSettings: ConsumerSettings[F, String, String],
      producerSettings: ProducerSettings[F, String, String],
      store: Ref[F, List[String]]
  ): PartitionedLoadExample[F, CommittableConsumerRecord[F, String, *]] = {

    val logger = LoggerFactory[F].getLogger

    implicit val partitionShow: Show[TopicPartition] = Show.show(tp => s"${tp.topic}-${tp.partition}")

    val loadStream =
      TopicLoader.partitionedLoad[F, String, String](topics, LoadCommitted, consumerSettings).map(_.value)

    val runStream = KafkaConsumer
      .stream(consumerSettings)
      .flatMap(_.partitionsMapStream.flatMap { partitionWithStream =>
        Stream
          .evalSeq(partitionWithStream.toList.traverse { case (partition, inputStream) =>
            logger.warn(s"Assigned ${partition.show}") *> inputStream.pure
          })
      }.parJoinUnbounded)

    val publishStream = publishAndCommit(producerSettings) { cr =>
      ProducerRecord(topic = outputTopic, key = cr.record.key, value = cr.record.value)
    }

    new PartitionedLoadExample(
      load = loadStream,
      run = runStream,
      publishAndCommit = publishStream,
      store = store
    )
  }
}
