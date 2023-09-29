package base

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import org.scalactic.source.Position
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.enablers.Retrying
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

trait AsyncIntSpec[F[_]] extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues with Eventually {
  override given patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  override given executionContext: ExecutionContext = ExecutionContext.global

  given fRetrying[T](using F: Async[F]): Retrying[F[T]] = new Retrying[F[T]] {
    override def retry(timeout: Span, interval: Span, pos: Position)(fun: => F[T]): F[T] =
      Dispatcher.sequential[F].use { dispatcher =>
        F.fromFuture(
          F.executionContext.map(
            Retrying.retryingNatureOfFutureT[T](_).retry(timeout, interval, pos)(dispatcher.unsafeToFuture(fun))
          )
        )
      }
  }
}
