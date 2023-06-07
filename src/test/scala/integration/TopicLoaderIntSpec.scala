package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.IO
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.effect.PropF
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted, LoadTopicStrategy}
import utils.RandomPort

import scala.concurrent.duration.*

class TopicLoaderIntSpec extends KafkaSpecBase[IO] {

  test("load when using LoadAll strategy should stream all records from all topics") {

    val strategy = LoadAll

    withKafkaContext { ctx =>
      import ctx.*

      val topics                 = NonEmptyList.of(testTopic1, testTopic2)
      val (forTopic1, forTopic2) = records(1 to 15).splitAt(10)

      for {
        _      <- createCustomTopics(topics, partitions = 2)
        _      <- publishStringMessages(testTopic1, forTopic1)
        _      <- publishStringMessages(testTopic2, forTopic2)
        result <- runLoader(topics, strategy)
      } yield assertEquals(result, forTopic1 ++ forTopic2)
    }

  }

  test("load when using LoadAll strategy should stream available records even when one topic is empty") {
    val strategy = LoadAll

    withKafkaContext { ctx =>
      import ctx.*

      val topics    = NonEmptyList.of(testTopic1, testTopic2)
      val published = records(1 to 15)

      for {
        _      <- createCustomTopics(topics)
        _      <- publishStringMessages(testTopic1, published)
        result <- runLoader(topics, strategy)
      } yield assertEquals(result, published)

    }
  }

  test("load when using LoadCommitted strategy should stream all records up to the committed offset") {
    val strategy = LoadCommitted

    withKafkaContext { ctx =>
      import ctx.*

      val topics                    = NonEmptyList.one(testTopic1)
      val (committed, notCommitted) = records(1 to 15).splitAt(10)

      for {
        partitions <- createCustomTopics(topics)
        _          <- publishStringMessages(testTopic1, committed)
        _          <- moveOffsetToEnd(partitions).compile.drain
        _          <- publishStringMessages(testTopic1, notCommitted)
        result     <- runLoader(topics, strategy)
      } yield assertEquals(result, committed)
    }

    test("load when using LoadCommitted strategy should stream available records even when one topic is empty") {
      val strategy = LoadCommitted

      withKafkaContext { ctx =>
        import ctx.*

        val topics                    = NonEmptyList.of(testTopic1, testTopic2)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        for {
          partitions <- createCustomTopics(topics)
          _          <- publishStringMessages(testTopic1, committed)
          _          <- moveOffsetToEnd(partitions).compile.drain
          _          <- publishStringMessages(testTopic1, notCommitted)
          result     <- runLoader(topics, strategy)
        } yield assertEquals(result, committed)
      }
    }

    test(
      "load when using LoadCommitted strategy should work when highest offset is missing in log and there are messages after highest offset"
    ) {
      val strategy = LoadCommitted

      withKafkaContext { ctx =>
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
        } yield assertEquals(result, notUpdated)
      }
    }
  }

  implicit val strategyArb: Arbitrary[LoadTopicStrategy] = Arbitrary(Gen.oneOf(LoadAll, LoadCommitted))

  test("using any strategy should complete successfully if the topic is empty") {
    PropF.forAllF { (strategy: LoadTopicStrategy) =>
      withKafkaContext { ctx =>
        import ctx.*

        val topics = NonEmptyList.one(testTopic1)

        for {
          result <- runLoader(topics, strategy)
        } yield assertEquals(result, List.empty)
      }
    }
  }

  test("using any strategy should complete successfully if the topic is empty") {
    PropF.forAllF { (strategy: LoadTopicStrategy) =>
      withKafkaContext { ctx =>
        import ctx.*

        val topics = NonEmptyList.one(testTopic1)

        for {
          result <- runLoader(topics, strategy)
        } yield assertEquals(result, List.empty)
      }
    }
  }

  test("using any strategy should read partitions that have been compacted") {
    PropF.forAllF { (strategy: LoadTopicStrategy) =>
      withKafkaContext { ctx =>
        import ctx.*

        val published        = records(1 to 10)
        val topic            = NonEmptyList.one(testTopic1)
        val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

        for {
          partitions <- createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)
          _          <- publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)
          result     <- runLoader(topic, strategy)
        } yield assert(!result.forall(published.contains(_)))
      }
    }
  }

  test("Kafka is misbehaving should fail if unavailable at startup") {
    withKafkaContext { _ =>
      val badConsumerSettings = ConsumerSettings[IO, String, String]
        .withBootstrapServers("localhost:6001")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("test-consumer-group")
        .withRequestTimeout(700.millis)
        .withSessionTimeout(500.millis)
        .withHeartbeatInterval(300.millis)
        .withDefaultApiTimeout(1000.millis)

      interceptIO[KafkaTimeoutException] {
        runLoader(NonEmptyList.one(testTopic1), LoadAll)(badConsumerSettings, IO.asyncForIO)
      }.void
    }
  }

  test("loadAndRun should execute callback when finished loading and keep streaming".ignore) {
    ()
  }

  private trait TestContext[F[_]] {
    implicit val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))
  }

  private def withKafkaContext(test: TestContext[IO] => IO[Unit]): IO[Unit] = {
    object testContext extends TestContext[IO]
    import testContext.*
    embeddedKafka.use(_ => test(testContext))
  }
}
