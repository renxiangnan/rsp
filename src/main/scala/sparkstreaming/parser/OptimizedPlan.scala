package sparkstreaming.parser

import org.openrdf.query.algebra.StatementPattern

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 24/05/16.
  */
class OptimizedPlan(collector: SparqlOpCollector) extends QueryLogicalPlan(collector) {

  // Concatenate all sub-expressions together
  override def createLogicalPlan: String = {

    val arraySP = new ArrayBuffer[StatementPattern]
    arraySP.appendAll(collector.opSPMap.keySet)

    createStarPatternExpr(arraySP).mkString("J")
  }


  def createStarPatternExpr(arraySP: ArrayBuffer[StatementPattern]): ArrayBuffer[String] = {

    val starSPMap = arraySP.groupBy(_.getSubjectVar.getName)
    val interExpr = new ArrayBuffer[ArrayBuffer[String]]
    val interExprJoin = new ArrayBuffer[ArrayBuffer[String]]
    val keys = starSPMap.keySet

    keys.foreach { key =>
      val tempSP = starSPMap(key).toArray.map(x => collector.opSPMap(x).toString)
      interExpr.append(tempSP.to[ArrayBuffer])
    }

    // Insert join operator "J"
    interExpr.foreach(x => {
      val xp = for (i <- x; p <- ArrayBuffer("J", i)) yield p
      interExprJoin += xp.tail
    })

    // Insert "()"
    interExprJoin.foreach { x =>
      x.insert(0, "(")
      x.insert(x.length, ")")
    }

    interExprJoin.map(x => x.mkString(""))
  }
}


object OptimizedPlan {
  def apply(collector: SparqlOpCollector): OptimizedPlan = new OptimizedPlan(collector)
}