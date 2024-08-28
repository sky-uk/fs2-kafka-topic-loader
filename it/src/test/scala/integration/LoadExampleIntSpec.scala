package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import fs2.kafka.*
import load.LoadExample
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.KafkaContainer.KafkaConfig
import utils.KafkaTestContainer

import scala.concurrent.duration.*

final class LoadExampleIntSpec extends KafkaSpecBase[IO], KafkaTestContainer[IO] {

  "LoadExample" should {
    "load previously seen messages into the store" in withKafkaContext { implicit kafkaConfig =>
      testContext { ctx =>
        import ctx.*

        for {
          _      <- publishStringMessage(inputTopic, "key1", "value1")
          _      <- runAppAndDiscard
          _      <- publishStringMessage(inputTopic, "key2", "value2")
          result <- runApp
        } yield result should contain theSameElementsInOrderAs List("value1", "value2")
      }
    }

    "not publish previously committed messages" in withKafkaContext { implicit kafkaConfig =>
      testContext { ctx =>
        import ctx.*

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
  }

  final case class TestContext()(using kafkaConfig: KafkaConfig) {

    private val store: IO[Ref[IO, List[String]]] = Ref.empty

    val inputTopic  = "test-topic-1"
    val outputTopic = "output-topic-1"

    private given LoggerFactory[IO] = Slf4jFactory.create[IO]

    given consumerSettings(using kafkaConfig: KafkaConfig): ConsumerSettings[IO, String, String] =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("load-example-consumer-group")

    given producerSettings(using kafkaConfig: KafkaConfig): ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

    val runApp: IO[List[String]] =
      for {
        store   <- store
        example1 = LoadExample.kafka[IO](
                     topics = NonEmptyList.one(inputTopic),
                     outputTopic = outputTopic,
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

  private def testContext(test: TestContext => IO[Assertion])(using KafkaConfig): IO[Assertion] = {
    val testContext = TestContext()

    test(testContext)
  }
}
