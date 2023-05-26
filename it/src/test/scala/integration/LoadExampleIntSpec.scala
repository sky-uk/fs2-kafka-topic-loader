package integration

import base.{AsyncIntSpecBase, KafkaSpecBase}
import cats.data.NonEmptyList
import cats.effect.{Async, IO, Ref}
import cats.syntax.all.*
import fs2.kafka.*
import load.LoadExample
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

class LoadExampleIntSpec extends AsyncIntSpecBase {

  "LoadExample" should {
    "load previously seen messages into the store" in {
      object testContext extends TestContext[IO]
      import testContext.*

      embeddedKafka.use { _ =>
        for {
          _         <- publishStringMessage(inputTopic, "key1", "value1")
          _         <- runAppAndDiscard()
          _         <- publishStringMessage(inputTopic, "key2", "value2")
          assertion <- runApp { result =>
                         result.asserting(_ should contain theSameElementsInOrderAs List("value1", "value2"))
                       }
        } yield assertion
      }
    }

    "not publish previously committed messages" in {
      object testContext extends TestContext[IO]
      import testContext.*

      embeddedKafka.use { _ =>
        for {
          _         <- publishStringMessage(inputTopic, "key1", "value1")
          _         <- runAppAndDiscard()
          _         <- consumeStringMessage(outputTopic, autoCommit = true)
          _         <- publishStringMessage(inputTopic, "key2", "value2")
          _         <- runAppAndDiscard()
          assertion <- consumeStringMessage(outputTopic, autoCommit = true).asserting(_ shouldBe "value2")
        } yield assertion
      }
    }
  }

  private abstract class TestContext[F[_] : Async] extends KafkaSpecBase[IO] {
    val inputTopic  = "test-topic-1"
    val outputTopic = "output-topic-1"

    private val store: F[Ref[F, List[String]]] = Ref[F].of(List.empty)

    private implicit val loggerFactory: LoggerFactory[F] = Slf4jFactory.create[F]

    private val timeout = 10.seconds

    private val consumerSettings: ConsumerSettings[F, String, String] =
      ConsumerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId("load-example-consumer-group")

    private val producerSettings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

    def runApp[T](f: F[List[String]] => F[T]): F[T] = {
      val storeState = for {
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

      f(storeState)
    }

    def runAppAndDiscard(): F[Unit] = runApp(identity).void
  }
}
