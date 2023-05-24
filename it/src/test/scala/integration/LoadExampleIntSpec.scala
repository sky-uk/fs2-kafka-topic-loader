package integration

import base.WordSpecBase
import cats.data.NonEmptyList
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.kafka.*
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import load.LoadExample
import org.scalatest.concurrent.Eventually
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.RandomPort

import scala.concurrent.duration.*

class LoadExampleIntSpec extends WordSpecBase with Eventually {

  "LoadExample" should {
    "load previously seen messages into the store before processing new messages" in new TestContext {

      withRunningKafka {

        publishToKafka(inputTopic, "key1", "value1")

        runApp { result =>
          result should contain theSameElementsInOrderAs List("value1")
        }

        eventually {
          consumeFirstStringMessageFrom(outputTopic, autoCommit = true) shouldBe "value1"
        }

        publishToKafka(inputTopic, "key2", "value2")

        runApp { result =>
          result should contain theSameElementsInOrderAs List("value1", "value2")
        }

        eventually {
          consumeFirstStringMessageFrom(outputTopic, autoCommit = true) shouldBe "value2"
        }
      }
    }
  }

  private trait TestContext extends EmbeddedKafka {
    val inputTopic  = "test-topic-1"
    val outputTopic = "output-topic-1"

    private val store: IO[Ref[IO, List[String]]] = Ref[IO].of(List.empty)

    private implicit val logger: Logger[IO] = Slf4jFactory.create[IO].getLogger

    private implicit lazy val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = RandomPort(), zooKeeperPort = RandomPort(), Map("log.roll.ms" -> "10"))

    private val timeout = 10.seconds

    private val consumerSettings: ConsumerSettings[IO, String, String] =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("load-example-consumer-group")

    private val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

    def runApp[T](f: List[String] => T): T = {
      val storeState = for {
        store   <- store
        example1 =
          LoadExample.kafka[IO](
            topics = NonEmptyList.one(inputTopic),
            outputTopic = outputTopic,
            consumerSettings = consumerSettings,
            producerSettings = producerSettings,
            store = store
          )
        _       <- example1.stream.interruptAfter(timeout).compile.drain
        stored  <- store.get
      } yield stored

      f(storeState.unsafeRunSync())
    }

  }
}
