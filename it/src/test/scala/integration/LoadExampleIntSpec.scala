package integration

import base.WordSpecBase
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Pipe
import fs2.kafka.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import load.LoadExample
import org.scalatest.concurrent.Eventually
import uk.sky.fs2.kafka.topicloader.{LoadCommitted, TopicLoader}
import utils.RandomPort

import scala.concurrent.duration.*

class LoadExampleIntSpec extends WordSpecBase with Eventually {

  "LoadExample" should {
    "load previously seen messages into the store before processing new messages" in new TestContext {

      val loadStream = TopicLoader.load[IO, String, String](topics, LoadCommitted, consumerSettings).map(_.value)

      val runStream = KafkaConsumer.stream(consumerSettings).subscribe(topics).records

      // TODO - combine with commit
      val publish: Pipe[IO, CommittableConsumerRecord[IO, String, String], Nothing] =
        _.map(message =>
          ProducerRecords.one(
            ProducerRecord(topic = outputTopic, key = message.record.key, value = message.record.value)
          )
        )
          .through(KafkaProducer.pipe(producerSettings))
          .drain

      val commit: Pipe[IO, CommittableConsumerRecord[IO, String, String], Nothing] =
        _.map(_.offset)
          .through(commitBatchWithin[IO](1, 5.seconds))
          .drain

      val example1 = new LoadExample(load = loadStream, run = runStream, publish = publish, commit = commit)

      withRunningKafka {
        publishToKafka(inputTopic, "key1", "value1")

        eventually {
          // TODO - find a way to finish the stream better
          example1.stream.interruptAfter(timeout).compile.drain.unsafeRunSync()

          example1.store should contain theSameElementsInOrderAs List("value1")

          consumeFirstStringMessageFrom(outputTopic, autoCommit = true) shouldBe "value1"
        }

        example1.store.clear()
        publishToKafka(inputTopic, "key2", "value2")

        eventually {
          example1.stream.interruptAfter(timeout).compile.drain.unsafeRunSync()

          example1.store should contain theSameElementsInOrderAs List("value1", "value2")

          consumeFirstStringMessageFrom(outputTopic, autoCommit = true) shouldBe "value2"
        }
      }

    }
  }

  private trait TestContext extends EmbeddedKafka {
    implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

    val timeout = 10.seconds

    val inputTopic = "test-topic-1"
    val topics     = NonEmptyList.one(inputTopic)

    val outputTopic = "output-topic-1"

    val groupId = "load-example-consumer-group"

    val consumerSettings: ConsumerSettings[IO, String, String] =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId(groupId)

    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

  }

}
