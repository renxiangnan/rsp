package sparkstreaming.producer

import org.openrdf.model.Model

/**
  * Created by xiangnanren on 25/05/16.
  */

abstract class RDFEvent

case class TripleEvent(model: Model) extends RDFEvent

case class ComplexEvent(streamId: String, eventId: String, timestamp: Long, model: Model) extends RDFEvent
