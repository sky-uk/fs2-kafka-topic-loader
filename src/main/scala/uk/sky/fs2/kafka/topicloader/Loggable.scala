package uk.sky.fs2.kafka.topicloader

import org.typelevel.log4cats.{Logger, LoggerFactory}

/** Convenience trait to avoid implementing overloads for LoggerFactory and Logger. This means library users don't have
  * to provide a specific one if the rest of their code uses the other.
  */
private sealed trait Loggable[F[_]] {
  def getLogger: Logger[F]
}

object Loggable {
  inline def apply[F[_] : Loggable]: Loggable[F]          = summon[Loggable[F]]
  private def apply[F[_]](logger: Logger[F]): Loggable[F] = new { override def getLogger: Logger[F] = logger }

  given fromLogger[F[_] : Logger]: Loggable[F]               = apply(Logger[F])
  given fromLoggerFactory[F[_] : LoggerFactory]: Loggable[F] = apply(LoggerFactory[F].getLogger)
}
