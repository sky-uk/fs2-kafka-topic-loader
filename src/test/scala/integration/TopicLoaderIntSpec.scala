package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{Async, IO}
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.scalatest.Assertion
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted}
import utils.RandomPort

import scala.concurrent.duration.*

class TopicLoaderIntSpec extends KafkaSpecBase[IO] {

  "load" when {

    "using LoadAll strategy" should {

      val strategy = LoadAll
      "stream all records from all topics" in withContext { ctx =>
        import ctx.*

        val topics                 = NonEmptyList.of(testTopic1, testTopic2)
        val (forTopic1, forTopic2) = records(1 to 15).splitAt(10)

        for {
          _      <- createCustomTopics(topics, partitions = 2)
          _      <- publishStringMessages(testTopic1, forTopic1)
          _      <- publishStringMessages(testTopic2, forTopic2)
          result <- runLoader(topics, strategy)
        } yield result should contain theSameElementsAs (forTopic1 ++ forTopic2)
      }

      "stream available records even when one topic is empty" in withContext { ctx =>
        import ctx.*

        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        for {
          _      <- createCustomTopics(topics)
          _      <- publishStringMessages(testTopic1, published)
          result <- runLoader(topics, strategy)
        } yield result should contain theSameElementsAs published

      }

    }

    "using LoadCommitted strategy" should {

      val strategy = LoadCommitted

      "stream all records up to the committed offset with LoadCommitted strategy" in withContext { ctx =>
        import ctx.*

        val topics                    = NonEmptyList.one(testTopic1)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        for {
          partitions <- createCustomTopics(topics)
          _          <- publishStringMessages(testTopic1, committed)
          _          <- moveOffsetToEnd(partitions).compile.drain
          _          <- publishStringMessages(testTopic1, notCommitted)
          result     <- runLoader(topics, strategy)
        } yield result should contain theSameElementsAs committed
      }

      "stream available records even when one topic is empty" in withContext { ctx =>
        import ctx.*

        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        for {
          partitions <- createCustomTopics(topics)
          _          <- publishStringMessages(testTopic1, published)
          _          <- moveOffsetToEnd(partitions).compile.drain
          result     <- runLoader(topics, strategy)
        } yield result should contain theSameElementsAs published
      }

      "work when highest offset is missing in log and there are messages after highest offset" in withContext { ctx =>
        import ctx.*

        val published                 = records(1 to 10)
        val (notUpdated, toBeUpdated) = published.splitAt(5)

        for {
          partitions <-
            createCustomTopics(NonEmptyList.one(testTopic1), partitions = 1, topicConfig = aggressiveCompactionConfig)
          _          <- publishStringMessages(testTopic1, published)
          _          <- moveOffsetToEnd(partitions).compile.drain
          _          <- publishToKafkaAndWaitForCompaction(partitions, toBeUpdated.map { case (k, v) => (k, v.reverse) })
          result     <- runLoader(NonEmptyList.one(testTopic1), strategy)
        } yield result should contain theSameElementsAs notUpdated
      }
    }

    "using any strategy" when {

      "strategy is LoadAll" should {

        val strategy = LoadAll

        "complete successfully if the topic is empty" in withContext { ctx =>
          import ctx.*

          val topics = NonEmptyList.one(testTopic1)

          for {
            result <- runLoader(topics, strategy)
          } yield result shouldBe empty
        }

        "read partitions that have been compacted" in withContext { ctx =>
          import ctx.*

          val published        = records(1 to 10)
          val topic            = NonEmptyList.one(testTopic1)
          val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

          for {
            partitions <- createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)
            _          <- publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)
            result     <- runLoader(topic, strategy)
          } yield result should contain noElementsOf published
        }
      }

      "strategy is LoadCommitted" should {

        val strategy = LoadCommitted

        "complete successfully if the topic is empty" in withContext { ctx =>
          import ctx.*

          val topics = NonEmptyList.one(testTopic1)

          for {
            result <- runLoader(topics, strategy)
          } yield result shouldBe empty
        }

        "read partitions that have been compacted" in withContext { ctx =>
          import ctx.*

          val published        = records(1 to 10)
          val topic            = NonEmptyList.one(testTopic1)
          val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

          for {
            partitions <- createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)
            _          <- publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)
            result     <- runLoader(topic, strategy)
          } yield result should contain noElementsOf published
        }
      }

    }

    "Kafka is misbehaving" should {

      "fail if unavailable at startup" in withContext { _ =>
        val badConsumerSettings = ConsumerSettings[IO, String, String]
          .withBootstrapServers("localhost:6001")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withGroupId("test-consumer-group")
          .withRequestTimeout(700.millis)
          .withSessionTimeout(500.millis)
          .withHeartbeatInterval(300.millis)
          .withDefaultApiTimeout(1000.millis)

        runLoader(NonEmptyList.one(testTopic1), LoadAll)(badConsumerSettings, IO.asyncForIO)
          .assertThrows[KafkaTimeoutException]
      }

    }
  }

  "loadAndRun" should {

    "execute callback when finished loading and keep streaming" in {
      pending
    }

  }

  private trait TestContext[F[_]] {
    implicit val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))
  }

  private def withContext(testCode: TestContext[IO] => IO[Assertion])(implicit F: Async[IO]): IO[Assertion] = {
    object testContext extends TestContext[IO]
    import testContext.*
    embeddedKafka.use(_ => testCode(testContext))
  }
}
