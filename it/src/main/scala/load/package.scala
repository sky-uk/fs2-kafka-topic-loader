import cats.effect.Async
import cats.syntax.all.*
import fs2.Pipe
import fs2.kafka.*

import scala.concurrent.duration.*

package object load {
  def publishAndCommit[F[_] : Async](producerSettings: ProducerSettings[F, String, String])(
      f: CommittableConsumerRecord[F, String, String] => ProducerRecord[String, String]
  ): Pipe[F, CommittableConsumerRecord[F, String, String], Nothing] =
    _.map(message => message.offset -> ProducerRecords.one(f(message))).through { offsetsAndProducerRecords =>
      KafkaProducer.stream(producerSettings).flatMap { producer =>
        offsetsAndProducerRecords.evalMap { case (offset, producerRecord) =>
          producer.produce(producerRecord).flatMap(_.as(offset))
        }
      }
    }.through(commitBatchWithin[F](1, 5.seconds)).drain
}
