package integration

import base.IntegrationSpecBase
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import io.github.embeddedkafka.Codecs.stringSerializer
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.prop.Tables.Table
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted, TopicLoader}

import scala.concurrent.duration.*

class TopicLoaderIntSpec extends IntegrationSpecBase {

  "load" when {

    "using LoadAll strategy" should {

      val strategy = LoadAll

      "stream all records from all topics" in new TestContext {
        val topics                 = NonEmptyList.of(testTopic1, testTopic2)
        val (forTopic1, forTopic2) = records(1 to 15).splitAt(10)

        withRunningKafka {
          createCustomTopics(topics, partitions = 2)

          publishToKafka(testTopic1, forTopic1)
          publishToKafka(testTopic2, forTopic2)

          val loadedRecords =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()

          loadedRecords.map(recordToTuple) should contain theSameElementsAs (forTopic1 ++ forTopic2)
        }

      }

      "stream available records even when one topic is empty" in new TestContext {
        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        withRunningKafka {
          createCustomTopics(topics)

          publishToKafka(testTopic1, published)

          val loadedRecords =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()

          loadedRecords.map(recordToTuple) should contain theSameElementsAs published
        }
      }

    }

    "using LoadCommitted strategy" should {

      val strategy = LoadCommitted

      "stream all records up to the committed offset with LoadCommitted strategy" in new TestContext with TestConsumer {
        val topics                    = NonEmptyList.one(testTopic1)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        withRunningKafka {
          val partitions = createCustomTopics(topics)

          publishToKafka(testTopic1, committed)

          moveOffsetToEnd(partitions).compile.drain.unsafeRunSync()

          publishToKafka(testTopic1, notCommitted)

          val loaded =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()

          loaded.map(recordToTuple) should contain theSameElementsAs committed
        }

      }

      "stream available records even when one topic is empty" in new TestContext with TestConsumer {
        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        withRunningKafka {
          val partitions = createCustomTopics(topics)

          publishToKafka(testTopic1, published)
          moveOffsetToEnd(partitions).compile.drain.unsafeRunSync()

          val loadedRecords =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()

          loadedRecords.map(recordToTuple) should contain theSameElementsAs published
        }
      }

      "work when highest offset is missing in log and there are messages after highest offset" in new TestContext
        with TestConsumer {
        val published                 = records(1 to 10)
        val (notUpdated, toBeUpdated) = published.splitAt(5)

        withRunningKafka {
          val partitions =
            createCustomTopics(NonEmptyList.one(testTopic1), partitions = 1, topicConfig = aggressiveCompactionConfig)

          publishToKafka(testTopic1, published)
          moveOffsetToEnd(partitions).compile.drain.unsafeRunSync()

          publishToKafkaAndWaitForCompaction(partitions, toBeUpdated.map { case (k, v) => (k, v.reverse) })

          val loadedRecords =
            TopicLoader
              .load[IO, String, String](NonEmptyList.one(testTopic1), strategy, consumerSettings)
              .compile
              .toList
              .unsafeRunSync()

          loadedRecords.map(recordToTuple) should contain theSameElementsAs notUpdated
        }
      }

    }

    "using any strategy" should {

      val loadStrategy = Table("strategy", LoadAll, LoadCommitted)

      "complete successfully if the topic is empty" in new TestContext {
        val topics = NonEmptyList.one(testTopic1)

        withRunningKafka {
          createCustomTopics(topics)

          forEvery(loadStrategy) { strategy =>
            TopicLoader
              .load[IO, String, String](topics, strategy, consumerSettings)
              .compile
              .toList
              .unsafeRunSync() shouldBe empty
          }
        }
      }

      "read partitions that have been compacted" in new TestContext with TestConsumer {
        val published        = records(1 to 10)
        val topic            = NonEmptyList.one(testTopic1)
        val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

        forEvery(loadStrategy) { strategy =>
          withRunningKafka {
            val partitions = createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)

            publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)

            val loadedRecords =
              TopicLoader.load[IO, String, String](topic, strategy, consumerSettings).compile.toList.unsafeRunSync()

            loadedRecords.map(recordToTuple) should contain noElementsOf published
          }
        }
      }

    }

    "Kafka is misbehaving" should {

      "fail if unavailable at startup" in new TestContext {
        val badConsumerSettings = ConsumerSettings[IO, String, String]
          .withBootstrapServers("localhost:6001")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withGroupId("test-consumer-group")
          .withRequestTimeout(700.millis)
          .withSessionTimeout(500.millis)
          .withHeartbeatInterval(300.millis)
          .withDefaultApiTimeout(1000.millis)

        val exception = intercept[Throwable](
          TopicLoader
            .load[IO, String, String](NonEmptyList.one(testTopic1), LoadAll, badConsumerSettings)
            .compile
            .drain
            .unsafeRunSync()
        )

        exception shouldBe a[KafkaTimeoutException]
      }

    }
  }

  "loadAndRun" should {

    "execute callback when finished loading and keep streaming" in {
      pending
    }

  }

}
