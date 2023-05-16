# fs2-kafka-topic-loader

Reads the contents of provided Kafka topics, either the topics in their entirety or up until a consumer groups last
committed Offset depending on which `LoadTopicStrategy` you provide.

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

## Configuration

Configuration from the Topic Loader is done via the `ConsumerSettings`. The group id of the Topic Loader should match
the group id of your application.

