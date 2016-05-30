package sparkstreaming.producer

/**
  * Created by xiangnanren on 25/05/16.
  */

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import org.apache.log4j.{Level, Logger}
import org.openrdf.model.Model


object RDFEventEmitter {
  val topic = "RDF-Event"
  val brokerAddr = "localhost:9092"

  def getProducerConfig(brokerAddr: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[MessageEncoder[Model]].getName)
    props.put("key.serializer.class", classOf[StringEncoder].getName)
    props
  }

  def sendMessages(topic: String, messages: List[RDFEvent], brokerAddr: String) {
    val producer = new Producer[String, RDFEvent](new ProducerConfig(getProducerConfig(brokerAddr)))
    producer.send(messages.map {
      new KeyedMessage[String, RDFEvent](topic, "RDF", _)
    }: _*)
    producer.close()
  }

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)

    println("Producer Launched")

    var obsId: Long = 0L
    while (true) {
      obsId += 1
      sendMessages(topic, List(RDFEventCreator.apply(obsId)), brokerAddr)
      Thread.sleep(1)
    }
  }
}