package uk.sky.fs2.kafka.topicloader

object TopicLoader extends TopicLoader

trait TopicLoader {

  def load(): Unit = ()

  def loadAndRun(): Unit = ()

}
