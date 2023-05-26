package integration

import base.{AsyncIntSpecBase, TestConsumer, TestContext}
import cats.data.NonEmptyList
import cats.effect.IO
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted}

import scala.concurrent.duration.*

class TopicLoaderIntSpec extends AsyncIntSpecBase {

  "load" when {

    "using LoadAll strategy" should {

      val strategy = LoadAll

      "stream all records from all topics" in {
        object testContext extends TestContext[IO]
        import testContext.*

        val topics                 = NonEmptyList.of(testTopic1, testTopic2)
        val (forTopic1, forTopic2) = records(1 to 15).splitAt(10)

        embeddedKafka.use { _ =>
          for {
            _         <- createCustomTopics(topics, partitions = 2)
            _         <- publishStringMessages(testTopic1, forTopic1)
            _         <- publishStringMessages(testTopic2, forTopic2)
            assertion <- runLoader(topics, strategy) { result =>
                           result.asserting(_ should contain theSameElementsAs (forTopic1 ++ forTopic2))
                         }
          } yield assertion
        }
      }

      "stream available records even when one topic is empty" in {
        object testContext extends TestContext[IO]
        import testContext.*

        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        embeddedKafka.use { _ =>
          for {
            _         <- createCustomTopics(topics)
            _         <- publishStringMessages(testTopic1, published)
            assertion <- runLoader(topics, strategy) { result =>
                           result.asserting(_ should contain theSameElementsAs published)
                         }
          } yield assertion
        }
      }

    }

    "using LoadCommitted strategy" should {

      val strategy = LoadCommitted

      "stream all records up to the committed offset with LoadCommitted strategy" in {
        object testContext extends TestConsumer[IO]
        import testContext.*

        val topics                    = NonEmptyList.one(testTopic1)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        embeddedKafka.use { _ =>
          for {
            partitions <- createCustomTopics(topics)
            _          <- publishStringMessages(testTopic1, committed)
            _          <- moveOffsetToEnd(partitions).compile.drain
            _          <- publishStringMessages(testTopic1, notCommitted)
            assertion  <- runLoader(topics, strategy) { result =>
                            result.asserting(_ should contain theSameElementsAs committed)
                          }
          } yield assertion
        }

      }

      "stream available records even when one topic is empty" in {
        object testContext extends TestConsumer[IO]
        import testContext.*

        val topics    = NonEmptyList.of(testTopic1, testTopic2)
        val published = records(1 to 15)

        embeddedKafka.use { _ =>
          for {
            partitions <- createCustomTopics(topics)
            _          <- publishStringMessages(testTopic1, published)
            _          <- moveOffsetToEnd(partitions).compile.drain
            assertion  <- runLoader(topics, strategy) { result =>
                            result.asserting(_ should contain theSameElementsAs published)
                          }
          } yield assertion
        }
      }

      "work when highest offset is missing in log and there are messages after highest offset" in {
        object testContext extends TestConsumer[IO]
        import testContext.*

        val published                 = records(1 to 10)
        val (notUpdated, toBeUpdated) = published.splitAt(5)

        embeddedKafka.use { _ =>
          for {
            partitions <-
              createCustomTopics(NonEmptyList.one(testTopic1), partitions = 1, topicConfig = aggressiveCompactionConfig)
            _          <- publishStringMessages(testTopic1, published)
            _          <- moveOffsetToEnd(partitions).compile.drain
            _          <- publishToKafkaAndWaitForCompaction(partitions, toBeUpdated.map { case (k, v) => (k, v.reverse) })
            assertion  <- runLoader(NonEmptyList.one(testTopic1), strategy) { result =>
                            result.asserting(_ should contain theSameElementsAs notUpdated)
                          }

          } yield assertion
        }
      }
    }

    /*
    TODO - Property testing doesn't work very well, even when using scalacheck-effect.
     Basically cats-effect-testing expects an IO[Assertion] but the `check()` from a PropF from scalacheck-effect returns
     a Future[TestResult].
     So we'd have to map on the Future and match on the inner TestResult returning a new assertion depending on if the
     match was a success or not.
     See this thread: https://github.com/typelevel/scalacheck-effect/issues/261
     */
    "using any strategy" when {

      "strategy is LoadAll" should {

        val strategy = LoadAll

        "complete successfully if the topic is empty" in {
          object testContext extends TestContext[IO]
          import testContext.*

          val topics = NonEmptyList.one(testTopic1)

          embeddedKafka.use { _ =>
            for {
              assertion <- runLoader(topics, strategy) { result =>
                             result.asserting(_ shouldBe empty)
                           }
            } yield assertion
          }
        }

        "read partitions that have been compacted" in {
          object testContext extends TestConsumer[IO]
          import testContext.*

          val published        = records(1 to 10)
          val topic            = NonEmptyList.one(testTopic1)
          val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

          embeddedKafka.use { _ =>
            for {
              partitions <- createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)
              _          <- publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)
              assertion  <- runLoader(topic, strategy) { result =>
                              result.asserting(_ should contain noElementsOf published)
                            }
            } yield assertion
          }
        }
      }

      "strategy is LoadCommitted" should {

        val strategy = LoadCommitted

        // TODO - duplicate test - see above
        "complete successfully if the topic is empty" in {
          object testContext extends TestContext[IO]
          import testContext.*

          val topics = NonEmptyList.one(testTopic1)

          embeddedKafka.use { _ =>
            for {
              assertion <- runLoader(topics, strategy) { result =>
                             result.asserting(_ shouldBe empty)
                           }
            } yield assertion
          }
        }

        // TODO - duplicate test - see above
        "read partitions that have been compacted" in {
          object testContext extends TestConsumer[IO]
          import testContext.*

          val published        = records(1 to 10)
          val topic            = NonEmptyList.one(testTopic1)
          val publishedUpdated = published.map { case (k, v) => (k, v.reverse) }

          embeddedKafka.use { _ =>
            for {
              partitions <- createCustomTopics(topic, partitions = 1, topicConfig = aggressiveCompactionConfig)
              _          <- publishToKafkaAndWaitForCompaction(partitions, published ++ publishedUpdated)
              assertion  <- runLoader(topic, strategy) { result =>
                              result.asserting(_ should contain noElementsOf published)
                            }
            } yield assertion
          }
        }
      }

    }

    "Kafka is misbehaving" should {

      "fail if unavailable at startup" in {
        object testContext extends TestContext[IO]
        import testContext.*

        val badConsumerSettings = ConsumerSettings[IO, String, String]
          .withBootstrapServers("localhost:6001")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withGroupId("test-consumer-group")
          .withRequestTimeout(700.millis)
          .withSessionTimeout(500.millis)
          .withHeartbeatInterval(300.millis)
          .withDefaultApiTimeout(1000.millis)

        for {
          assertion <- runLoader(NonEmptyList.one(testTopic1), LoadAll, badConsumerSettings) { result =>
                         result.assertThrows[KafkaTimeoutException]
                       }
        } yield assertion
      }

    }
  }

  "loadAndRun" should {

    "execute callback when finished loading and keep streaming" in {
      pending
    }

  }

}
