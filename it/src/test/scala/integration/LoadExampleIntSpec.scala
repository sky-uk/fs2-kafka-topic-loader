package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import fs2.kafka.*
import io.github.embeddedkafka.EmbeddedKafkaConfig
import load.LoadExample
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

final class LoadExampleIntSpec extends KafkaSpecBase[IO] {

  "LoadExample" should {
    "load previously seen messages into the store" in withKafkaContext { ctx =>
      import ctx.{*, given}

      for {
        _      <- publishStringMessage(inputTopic, "key1", "value1")
        _      <- runAppAndDiscard
        _      <- publishStringMessage(inputTopic, "key2", "value2")
        result <- runApp
      } yield result should contain theSameElementsInOrderAs List("value1", "value2")
    }

    "not publish previously committed messages" in withKafkaContext { ctx =>
      import ctx.{*, given}

      for {
        _      <- publishStringMessage(inputTopic, "key1", "value1")
        _      <- runAppAndDiscard
        _      <- consumeStringMessage(outputTopic, autoCommit = true)
        _      <- publishStringMessage(inputTopic, "key2", "value2")
        _      <- runAppAndDiscard
        result <- consumeStringMessage(outputTopic, autoCommit = true)
      } yield result shouldBe "value2"
    }
  }

  private trait TestContext {

    private val store: IO[Ref[IO, List[String]]] = Ref.empty

    val inputTopic  = "test-topic-1"
    val outputTopic = "output-topic-1"

    private given LoggerFactory[IO] = Slf4jFactory.create[IO]

    given kafkaConfig: EmbeddedKafkaConfig

    val consumerSettings: ConsumerSettings[IO, String, String]

    val producerSettings: ProducerSettings[IO, String, String]

    val runApp: IO[List[String]] =
      for {
        store   <- store
        example1 = LoadExample.kafka[IO](
                     topics = NonEmptyList.one(inputTopic),
                     outputTopic = outputTopic,
                     consumerSettings = consumerSettings,
                     producerSettings = producerSettings,
                     store = store
                   )
        _       <- example1.stream
                     .interruptAfter(10.seconds)
                     .compile
                     .drain
        stored  <- store.get
      } yield stored

    val runAppAndDiscard: IO[Unit] = runApp.void
  }

  private def withKafkaContext(test: TestContext => IO[Assertion]): IO[Assertion] =
    for {
      config     <- embeddedKafkaConfigF
      testContext = new TestContext {
                      override given kafkaConfig: EmbeddedKafkaConfig = config

                      override val consumerSettings: ConsumerSettings[IO, String, String] =
                        ConsumerSettings[IO, String, String]
                          .withBootstrapServers(s"localhost:${config.kafkaPort}")
                          .withAutoOffsetReset(AutoOffsetReset.Earliest)
                          .withGroupId("load-example-consumer-group")

                      override val producerSettings: ProducerSettings[IO, String, String] =
                        ProducerSettings[IO, String, String]
                          .withBootstrapServers(s"localhost:${config.kafkaPort}")
                    }
      assertion  <- embeddedKafkaR(config).surround(test(testContext))
    } yield assertion
}
