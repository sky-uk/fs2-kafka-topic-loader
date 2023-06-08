# fs2-kafka-topic-loader

Reads the contents of provided Kafka topics determined by the `LoadTopicStrategy`.

- `LoadAll` - reads the topics in their entirety
- `LoadCommitted` - reads up to the configured consumer-group's last committed Offset

This library is aimed for usage in applications that want a deterministic stream of Kafka messages that completes once
the last message (determined above) has been read. This is useful if an application shouldn't respond to new events
before it has processed all previously seen messages, or if Kafka is being used as a data store and the entire contents
of a topic needs to be reloaded on an application restart.

## Usage

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

See [`LoadExample.scala`](./it/src/main/scala/load/LoadExample.scala) for a more detailed example.

## Configuration

Configuration from the Topic Loader is done via the `ConsumerSettings`. The group id of the Topic Loader should match
the group id of your application.

## Contributing

Contributions are welcomed! For contributions see [here](./CONTRIBUTING.md) for more information.