package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import org.scalatest.Assertion
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted}

import scala.concurrent.duration.*

class TopicLoaderIntSpec extends KafkaSpecBase[IO] {

  "load" when {

    "using LoadAll strategy" should {

      val strategy = LoadAll
      "stream all records from all topics" in withKafkaContext { ctx =>
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

      "stream available records even when one topic is empty" in withKafkaContext { ctx =>
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

      "stream all records up to the committed offset with LoadCommitted strategy" in withKafkaContext { ctx =>
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

      "stream available records even when one topic is empty" in withKafkaContext { ctx =>
        import ctx.*

        val topics                    = NonEmptyList.of(testTopic1, testTopic2)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        for {
          partitions <- createCustomTopics(topics)
          _          <- publishStringMessages(testTopic1, committed)
          _          <- moveOffsetToEnd(partitions).compile.drain
          _          <- publishStringMessages(testTopic1, notCommitted)
          result     <- runLoader(topics, strategy)
        } yield result should contain theSameElementsAs committed
      }

      "work when highest offset is missing in log and there are messages after highest offset" in withKafkaContext {
        ctx =>
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

        "complete successfully if the topic is empty" in withKafkaContext { ctx =>
          import ctx.*

          val topics = NonEmptyList.one(testTopic1)

          for {
            result <- runLoader(topics, strategy)
          } yield result shouldBe empty
        }

        "read partitions that have been compacted" in withKafkaContext { ctx =>
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

        "complete successfully if the topic is empty" in withKafkaContext { ctx =>
          import ctx.*

          val topics = NonEmptyList.one(testTopic1)

          for {
            result <- runLoader(topics, strategy)
          } yield result shouldBe empty
        }

        "read partitions that have been compacted" in withKafkaContext { ctx =>
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

      "fail if unavailable at startup" in withKafkaContext { _ =>
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

    "execute callback when finished loading and keep streaming" in withKafkaContext { ctx =>
      import ctx.*

      val (preLoad, postLoad) = records(1 to 15).splitAt(10)

      for {
        loadState  <- Ref.of[IO, Boolean](false)
        topicState <- Ref.empty[IO, Seq[(String, String)]]
        _          <- createCustomTopics(NonEmptyList.one(testTopic1))
        _          <- publishStringMessages(testTopic1, preLoad)
        assertion  <- loadAndRunR(NonEmptyList.one(testTopic1))(
                        _ => loadState.set(true),
                        r => topicState.getAndUpdate(_ :+ r).void
                      ).surround {
                        for {
                          _         <- eventually(topicState.get.asserting(_ should contain theSameElementsAs preLoad))
                          _         <- loadState.get.asserting(_ shouldBe true)
                          _         <- publishStringMessages(testTopic1, postLoad)
                          assertion <-
                            eventually(
                              topicState.get.asserting(_ should contain theSameElementsAs (preLoad ++ postLoad))
                            )
                        } yield assertion
                      }
      } yield assertion
    }

  }

  private trait TestContext[F[_]] {
    implicit val kafkaConfig: EmbeddedKafkaConfig
  }

  private def withKafkaContext(test: TestContext[IO] => IO[Assertion]): IO[Assertion] =
    for {
      config     <- embeddedKafkaConfigF
      testContext = new TestContext[IO] {
                      override implicit val kafkaConfig: EmbeddedKafkaConfig = config
                    }
      assertion  <- {
        import testContext.*
        embeddedKafkaR.surround(test(testContext))
      }
    } yield assertion
}
