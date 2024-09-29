package utils

import cats.effect.Async
import org.scalatest.Assertion
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import utils.KafkaServer.KafkaConfig

trait KafkaTestContainer[F[_]] {
  def withRunningKafka(test: KafkaConfig => F[Assertion])(using Async[F]): F[Assertion] = {
    given LoggerFactory[F] = Slf4jFactory.create[F]
    KafkaServer[F].use(running => test(running.config))
  }
}
