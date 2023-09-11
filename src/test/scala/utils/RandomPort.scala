package utils

import java.net.ServerSocket

import cats.effect.{Resource, Sync}

object RandomPort {
  def apply[F[_]](implicit F: Sync[F]): F[Int] =
    Resource.fromAutoCloseable(F.delay(new ServerSocket(0))).use { socket =>
      F.delay {
        socket.setReuseAddress(true)
        socket.getLocalPort
      }
    }
}
