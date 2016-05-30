package sparkstreaming.parser

import org.openrdf.query.algebra.StatementPattern

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 24/05/16.
  */
class UnResolvedPlan(collector: SparqlOpCollector) extends QueryLogicalPlan(collector) {

  override def createLogicalPlan: String = {
    val expr = new ArrayBuffer[String]

    collector.opSPMap.foreach { case (key: StatementPattern, value: Int) =>
      expr.isEmpty match {
        case true => expr.append(value.toString)
        case false => expr.append("J"); expr.append(value.toString)
      }
    }
    expr.mkString("")
  }
}


object UnResolvedPlan {
  def apply(collector: SparqlOpCollector) = new UnResolvedPlan(collector: SparqlOpCollector)
}