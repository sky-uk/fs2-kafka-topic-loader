[![Build Status](https://app.travis-ci.com/sky-uk/fs2-kafka-topic-loader.svg?branch=main)](https://app.travis-ci.com/sky-uk/fs2-kafka-topic-loader)
[![Maven Central](https://img.shields.io/maven-central/v/uk.sky/fs2-kafka-topic-loader_3)](https://mvnrepository.com/artifact/uk.sky/fs2-kafka-topic-loader)
[![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/uk.sky/fs2-kafka-topic-loader_3?label=snapshot&server=https%3A%2F%2Fs01.oss.sonatype.org)](https://s01.oss.sonatype.org/content/repositories/snapshots/uk/sky/fs2-kafka-topic-loader_3/)

# fs2-kafka-topic-loader

Reads the contents of provided Kafka topics.

This library is aimed for usage in applications that want a deterministic stream of Kafka messages that completes once
the last message (determined above) has been read. This is useful if an application shouldn't respond to new events
before it has processed all previously seen messages, or if Kafka is being used as a data store and the entire contents
of a topic needs to be reloaded on an application restart.

## Usage

### `Load`

Determined by the `LoadTopicStrategy`, this stream completes once all messages have been read.

Load strategies:

- `LoadAll` - reads the topics in their entirety
- `LoadCommitted` - reads up to the configured consumer-group's last committed Offset

### `LoadAndRun`

Read up to the latest offset, fire a callback and continue streaming messages.

## Example

Add the following to your `build.sbt`:

```scala
libraryDependencies += "uk.sky" %% "fs2-kafka-topic-loader" % "<version>"
```

### Load

```scala
import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import fs2.kafka.ConsumerSettings
import org.typelevel.log4cats.LoggerFactory
import uk.sky.fs2.kafka.topicloader.{LoadAll, TopicLoader}

object Main extends IOApp.Simple {
  val consumerSettings: ConsumerSettings[IO, String, String] = ???

  given LoggerFactory[IO] = ???

  override def run: IO[Unit] =
    TopicLoader.load(NonEmptyList.one("topicToLoad"), LoadAll, consumerSettings).evalTap(IO.println).compile.drain
}
```

See [`LoadExample.scala`](./it/src/main/scala/load/LoadExample.scala) for a more detailed example.

### LoadAndRun

```scala
import cats.data.NonEmptyList
import cats.effect.kernel.Resource.ExitCase
import cats.effect.{IO, IOApp, Ref}
import fs2.kafka.ConsumerSettings
import org.typelevel.log4cats.LoggerFactory
import uk.sky.fs2.kafka.topicloader.{LoadAll, TopicLoader}

object Main extends IOApp.Simple {
  val consumerSettings: ConsumerSettings[IO, String, String] = ???

  given LoggerFactory[IO] = ???

  val healthCheck: IO[Ref[IO, Boolean]] = Ref.of(false)

  val logger = LoggerFactory[IO].getLogger

  override def run: IO[Unit] =
    for {
      healthCheck <- healthCheck
      _           <- TopicLoader
                       .loadAndRun(NonEmptyList.one("topicToLoad"), consumerSettings) {
                         case ExitCase.Succeeded  => healthCheck.set(true)
                         case ExitCase.Errored(e) => logger.error(e)(s"Something went wrong: $e")
                         case ExitCase.Canceled   => logger.warn("Stream was cancelled before loading")
                       }
                       .compile
                       .drain
    } yield ()
}
```

## Configuration

Configuration from the Topic Loader is done via the `ConsumerSettings`. The group id of the Topic Loader should match
the group id of your application.

## Contributing

Contributions are welcomed! For contributions see [here](./CONTRIBUTING.md) for more information.
