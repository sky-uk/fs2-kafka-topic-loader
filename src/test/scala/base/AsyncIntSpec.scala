package base

import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*

trait AsyncIntSpec extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues with Eventually {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(200.millis, 10.seconds)
}
