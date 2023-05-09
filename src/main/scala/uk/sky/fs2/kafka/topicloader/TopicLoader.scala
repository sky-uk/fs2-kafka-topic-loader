package uk.sky.fs2.kafka.topicloader

import cats.data.NonEmptyList
import cats.effect.Async
import fs2.Stream
import fs2.kafka.{ConsumerRecord, ConsumerSettings}
import org.apache.kafka.common.serialization.Deserializer

object TopicLoader extends TopicLoader

trait TopicLoader {

  def load[F[_] : Async, K : Deserializer, V : Deserializer](
      topics: NonEmptyList[String],
      strategy: LoadTopicStrategy,
      consumerSettings: ConsumerSettings[F, Array[Byte], Array[Byte]]
  ): Stream[F, ConsumerRecord[K, V]] = ???

  def loadAndRun(): Unit = ()

}
