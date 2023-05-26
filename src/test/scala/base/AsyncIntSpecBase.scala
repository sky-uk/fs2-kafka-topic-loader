package base

import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

abstract class AsyncIntSpecBase extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues
