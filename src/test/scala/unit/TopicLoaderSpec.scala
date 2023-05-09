package unit

import base.UnitSpecBase
import uk.sky.fs2.kafka.topicloader.TopicLoader

class TopicLoaderSpec extends UnitSpecBase {

  "TopicLoader.load" should {
    "doNothing" in {
      TopicLoader.load() shouldBe ()
    }
  }

  "TopicLoader.loadAndRun" should {
    "doNothing" in {
      TopicLoader.loadAndRun() shouldBe ()
    }
  }

}
