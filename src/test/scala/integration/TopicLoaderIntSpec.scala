package integration

import base.IntegrationSpecBase

class TopicLoaderIntSpec extends IntegrationSpecBase {

  "load" when {

    "using LoadAll strategy" should {

      "stream all records from all topics" in {
        pending
      }

      "stream available records even when one topic is empty" in {
        pending
      }

    }

    "using LoadCommitted strategy" should {

      "stream all records up to the committed offset with LoadCommitted strategy" in {
        pending
      }

      "stream available records even when one topic is empty" in {
        pending
      }

      "work when highest offset is missing in log and there are messages after highest offset" in {
        pending
      }

    }

    "using any strategy" should {

      "complete successfully if the topic is empty" in {
        pending
      }

      "read partitions that have been compacted" in {
        pending
      }

    }

    "Kafka is misbehaving" should {

      "fail if unavailable at startup" in {
        pending
      }

    }
  }

  "loadAndRun" should {

    "execute callback when finished loading and keep streaming" in {
      pending
    }

  }

}
