package sparkstreaming.consumer

/**
  * Created by xiangnanren on 25/05/16.
  */

import org.openrdf.model.Model
import sparkstreaming.producer.{ComplexEvent, RDFEvent, TripleEvent}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


object MessageMapper {
  def eventMapper(event: RDFEvent) = {
    event match {
      case TripleEvent(model: Model) =>
        model.map(x => Stream(x.getSubject.stringValue, x.getPredicate.stringValue, x.getObject.stringValue))

      case ComplexEvent(streamId: String, eventId: String, timeStamp: Long, model: Model) =>
        model.map(x => Stream(streamId, eventId, timeStamp.toString,
          x.getSubject.stringValue, x.getPredicate.stringValue, x.getObject.stringValue))
    }
  }
}
