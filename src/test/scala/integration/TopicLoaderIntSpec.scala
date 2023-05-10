package integration

import base.IntegrationSpecBase
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.github.embeddedkafka.Codecs.{stringDeserializer, stringSerializer}
import uk.sky.fs2.kafka.topicloader.{LoadAll, LoadCommitted, TopicLoader}

class TopicLoaderIntSpec extends IntegrationSpecBase {

  "load" when {

    "using LoadAll strategy" should {

      val strategy = LoadAll

      "stream all records from all topics" in new TestContext {
//        pending
        val topics                 = NonEmptyList.of(testTopic1, testTopic2)
        val (forTopic1, forTopic2) = records(1 to 15).splitAt(10)

        withRunningKafka {
          createCustomTopics(topics, 1)

          publishToKafka(testTopic1, forTopic1)
          publishToKafka(testTopic2, forTopic2)

          val loadedRecords =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()

          loadedRecords.map(recordToTuple) should contain theSameElementsAs (forTopic1 ++ forTopic2)
        }

      }

      "stream available records even when one topic is empty" in new TestContext {
        pending
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

      "stream all records up to the committed offset with LoadCommitted strategy" in new TestContext with Consumer {
//        pending
        val topics                    = NonEmptyList.one(testTopic1)
        val (committed, notCommitted) = records(1 to 15).splitAt(10)

        withRunningKafka {
          createCustomTopics(topics, partitions = 1)

          publishToKafka(testTopic1, committed)
          println(s"publish committed")

          moveOffsetToEnd(testTopic1).compile.drain.unsafeRunSync()
          println(s"moved to end")

          publishToKafka(testTopic1, notCommitted)
          println(s"publish not committed")

          val loaded =
            TopicLoader.load[IO, String, String](topics, strategy, consumerSettings).compile.toList.unsafeRunSync()
          println(s"loaded $loaded")

          loaded.map(recordToTuple) should contain theSameElementsAs committed
        }

      }

      "stream available records even when one topic is empty" in {
        pending
      }

      "work when highest offset is missing in log and there are messages after highest offset" in {
        pending
      }

    }

    "using any strategy" should {

      "complete successfully if the topic is empty" in {
        pending
      }

      "read partitions that have been compacted" in {
        pending
      }

    }

    "Kafka is misbehaving" should {

      "fail if unavailable at startup" in {
        pending
      }

    }
  }

  "loadAndRun" should {

    "execute callback when finished loading and keep streaming" in {
      pending
    }

  }

}
