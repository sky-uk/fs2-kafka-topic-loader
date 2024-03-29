package load

import cats.Traverse
import cats.data.NonEmptyList
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.kafka.*
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.LoggerFactory
import uk.sky.fs2.kafka.topicloader.{LoadCommitted, TopicLoader}

import scala.concurrent.duration.*

/** Simple application that streams Kafka records into a memory store, and produce the same record back to Kafka. On
  * startup, it will read all of the currently committed records back into this store, but not produce them to Kafka.
  */
class LoadExample[F[_] : Async, G[_] : Traverse, O](
    load: Stream[F, String],
    run: Stream[F, G[String]],
    publishAndCommit: Pipe[F, G[String], O],
    store: Ref[F, List[String]]
) {
  private def process(message: String): F[Unit] = store.update(_ :+ message)

  val stream: Stream[F, O] =
    (load.evalTap(process).drain ++ run.evalTap(_.traverse(process)))
      .through(publishAndCommit)

}

object LoadExample {

  type Message[F[_], A] = CommittableConsumerRecord[F, String, A]

  def kafka[F[_] : Async : LoggerFactory](
      topics: NonEmptyList[String],
      outputTopic: String,
      consumerSettings: ConsumerSettings[F, String, String],
      producerSettings: ProducerSettings[F, String, String],
      store: Ref[F, List[String]]
  ): LoadExample[F, Message[F, *], Unit] = {
    val loadStream = TopicLoader.load[F, String, String](topics, LoadCommitted, consumerSettings).map(_.value)

    val runStream = KafkaConsumer.stream(consumerSettings).subscribe(topics).records

    val publishAndCommit: Pipe[F, CommittableConsumerRecord[F, String, String], Unit] =
      _.map(message =>
        message.offset -> ProducerRecords.one(
          ProducerRecord(topic = outputTopic, key = message.record.key, value = message.record.value)
        )
      ).through { offsetsAndProducerRecords =>
        KafkaProducer.stream(producerSettings).flatMap { producer =>
          offsetsAndProducerRecords.evalMap { case (offset, producerRecord) =>
            producer.produce(producerRecord).flatMap(_.as(offset))
          }
        }
      }.through(commitBatchWithin[F](1, 5.seconds))

    new LoadExample(
      load = loadStream,
      run = runStream,
      publishAndCommit = publishAndCommit,
      store = store
    )
  }
}
