package uk.sky.fs2.kafka.topicloader

import org.typelevel.log4cats.{Logger, LoggerFactory}

import scala.annotation.implicitNotFound

/** Convenience trait to avoid implementing overloads for LoggerFactory and Logger. This means library users don't have
  * to provide a specific one if the rest of their code uses the other.
  */
@implicitNotFound("""
Implicit not found for Loggable[${F}].
An implicit org.typelevel.log4cats.Logger[${F}] or org.typelevel.log4cats.LoggerFactory[${F}] must be in scope
""")
private sealed trait Loggable[F[_]] {
  def getLogger: Logger[F]
}

private object Loggable {
  inline def apply[F[_] : Loggable]: Loggable[F] = summon[Loggable[F]]

  given fromLogger[F[_] : Logger]: Loggable[F]               = apply(Logger[F])
  given fromLoggerFactory[F[_] : LoggerFactory]: Loggable[F] = apply(LoggerFactory[F].getLogger)

  inline private def apply[F[_]](logger: Logger[F]): Loggable[F] = new { val getLogger: Logger[F] = logger }
}
