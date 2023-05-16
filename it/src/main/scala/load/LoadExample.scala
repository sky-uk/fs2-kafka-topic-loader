package load

import cats.Traverse
import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.{Pipe, Stream}

import scala.collection.mutable.ListBuffer

class LoadExample[F[_] : Async, G[_] : Traverse](
    load: Stream[F, String],
    run: Stream[F, G[String]],
    publish: Pipe[F, G[String], Nothing],
    commit: Pipe[F, G[String], Nothing]
) {

  // TODO - use cats ref
  val store: ListBuffer[String] = ListBuffer.empty

  private def process(message: String): F[Unit] = Async[F].pure(store.append(message))

  val stream: Stream[F, G[String]] =
    (load.evalTap(process).drain ++ run.evalTap(_.traverse(process)))
      .observe(publish)
      .observe(commit)

}
