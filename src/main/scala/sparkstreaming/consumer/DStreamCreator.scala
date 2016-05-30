package sparkstreaming.consumer

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import sparkstreaming.producer.{MessageDecoder, RDFEvent}

/**
  * Created by xiangnanren on 26/05/16.
  */
class DStreamCreator(ssc: StreamingContext, cc: ConsumerConfigurator) {

  val stream = KafkaUtils.createStream[String, RDFEvent, StringDecoder,
    MessageDecoder[RDFEvent]](ssc, cc.kafkaParams, Map(cc.topic -> 1), StorageLevel.MEMORY_AND_DISK)

  val message = stream.map(x => x._2)

  val dstream = message.map(x => MessageMapper.eventMapper(x))

}

object DStreamCreator {
  def apply(ssc: StreamingContext, cc: ConsumerConfigurator): DStreamCreator
  = new DStreamCreator(ssc, cc)
}