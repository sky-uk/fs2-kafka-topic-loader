package load

import cats.Traverse
import cats.data.NonEmptyList
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.kafka.*
import fs2.{Pipe, Stream}
import uk.sky.fs2.kafka.topicloader.{LoadCommitted, TopicLoader}

import scala.concurrent.duration.*

class LoadExample[F[_] : Async, G[_] : Traverse](
    load: Stream[F, String],
    run: Stream[F, G[String]],
    publishAndCommit: Pipe[F, G[String], Nothing],
    store: Ref[F, List[String]]
) {
  private def process(message: String): F[Unit] = store.update(_ :+ message)

  val stream: Stream[F, G[String]] =
    (load.evalTap(process).drain ++ run.evalTap(_.traverse(process))).observe(publishAndCommit)

}

object LoadExample {
  def kafka[F[_] : Async](
      topics: NonEmptyList[String],
      outputTopic: String,
      consumerSettings: ConsumerSettings[F, String, String],
      producerSettings: ProducerSettings[F, String, String],
      store: Ref[F, List[String]]
  ): LoadExample[F, CommittableConsumerRecord[F, String, *]] = {
    val loadStream = TopicLoader.load[F, String, String](topics, LoadCommitted, consumerSettings).map(_.value)

    val runStream = KafkaConsumer.stream(consumerSettings).subscribe(topics).records

    val publishAndCommit: Pipe[F, CommittableConsumerRecord[F, String, String], Nothing] =
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
      }.through(commitBatchWithin[F](1, 5.seconds)).drain

    new LoadExample(
      load = loadStream,
      run = runStream,
      publishAndCommit = publishAndCommit,
      store = store
    )
  }
}
