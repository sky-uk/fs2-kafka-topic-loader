# fs2-kafka-topic-loader

Reads the contents of provided Kafka topics, in one of two ways: 
- reads the topics in their entirety  
- reads up to the last consumer group's committed Offset 
This is determined by the `LoadTopicStrategy`.

Add the following to your `build.sbt`:

```scala
libraryDependencies += "uk.sky" %% "fs2-kafka-topic-loader" % "<version>"
```

```scala
import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import fs2.kafka.ConsumerSettings

object Main extends IOApp.Simple {
  val consumerSettings: ConsumerSettings[IO, String, String] = ???

  override def run: IO[Unit] =
    TopicLoader.load(NonEmptyList.one("topicToLoad"), LoadAll, consumerSettings).evalTap(IO.println).compile.drain
}
```

See [LoadExample.scala](./it/src/main/scala/load/LoadExample.scala) for a more detailed example.

## Configuration

Configuration from the Topic Loader is done via the `ConsumerSettings`. The group id of the Topic Loader should match
the group id of your application.

