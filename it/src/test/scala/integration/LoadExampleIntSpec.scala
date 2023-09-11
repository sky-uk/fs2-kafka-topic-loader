package integration

import base.KafkaSpecBase
import cats.data.NonEmptyList
import cats.effect.{Async, IO, Ref}
import cats.syntax.all.*
import fs2.kafka.*
import io.github.embeddedkafka.EmbeddedKafkaConfig
import load.LoadExample
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

class LoadExampleIntSpec extends KafkaSpecBase[IO] {
  val inputTopic      = "test-topic-1"
  val outputTopic     = "output-topic-1"
  private val timeout = 10.seconds

  "LoadExample" should {
    "load previously seen messages into the store" in withKafkaContext { ctx =>
      import ctx.*

      for {
        _      <- publishStringMessage(inputTopic, "key1", "value1")
        _      <- runAppAndDiscard
        _      <- publishStringMessage(inputTopic, "key2", "value2")
        result <- runApp
      } yield result should contain theSameElementsInOrderAs List("value1", "value2")
    }

    "not publish previously committed messages" in withKafkaContext { ctx =>
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

  private abstract class TestContext[F[_] : Async] {

    implicit val kafkaConfig: EmbeddedKafkaConfig

    private val store: F[Ref[F, List[String]]] = Ref[F].of(List.empty)

    private implicit val loggerFactory: LoggerFactory[F] = Slf4jFactory.create[F]

    private lazy val consumerSettings: ConsumerSettings[F, String, String] =
      ConsumerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("load-example-consumer-group")

    private lazy val producerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

    val runApp: F[List[String]] =
      for {
        store   <- store
        example1 =
          LoadExample.kafka[F](
            topics = NonEmptyList.one(inputTopic),
            outputTopic = outputTopic,
            consumerSettings = consumerSettings,
            producerSettings = producerSettings,
            store = store
          )
        stored  <- example1.stream.interruptAfter(timeout).compile.drain *> store.get
      } yield stored

    val runAppAndDiscard: F[Unit] = runApp.void
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
