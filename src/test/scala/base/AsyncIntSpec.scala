package base

import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

trait AsyncIntSpec extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues
