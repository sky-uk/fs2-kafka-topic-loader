package load

import cats.Traverse
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all.*
import fs2.{Pipe, Stream}

class LoadExample[F[_] : Async, G[_] : Traverse](
    load: Stream[F, String],
    run: Stream[F, G[String]],
    publishAndCommit: Pipe[F, G[String], Nothing],
    store: Ref[F, List[String]]
) {
  private def process(message: String): F[Unit] = store.update(_ :+ message)

  val stream: Stream[F, G[String]] =
    (load.evalTap(process).drain ++ run.evalTap(_.traverse(process))).observe(publishAndCommit)

}
