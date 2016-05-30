package sparkstreaming.consumer

/**
  * Created by xiangnanren on 26/05/16.
  */

class ConsumerConfigurator {

  val (topic, groupId): (String, String) = ("RDF-Event", "test-consumer-group")
  val kafkaParams = Map("zookeeper.connect" -> "localhost:2181",
    "group.id" -> groupId,
    "auto.offset.reset" -> "smallest")
}


object ConsumerConfigurator {
  def apply: ConsumerConfigurator = new ConsumerConfigurator()
}
