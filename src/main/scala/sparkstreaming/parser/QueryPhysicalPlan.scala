package sparkstreaming.parser

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by xiangnanren on 24/05/16.
  */
class QueryPhysicalPlan(expr: String) {
  private val root = parseToTree(expr)

  def parseToTree(expr: String): QueryPhysicalPlan.Node = {
    var i = expr.length - 1
    var parentCount = 0
    var opLoc = -1

    while (i > 0) {
      if (expr(i) == ')') parentCount += 1
      else if (expr(i) == '(') parentCount -= 1
      else if (parentCount == 0 && (expr(i) == 'J')) {
        opLoc = i
        i = -1
      }
      i -= 1
    }

    if (opLoc < 0) {
      if (expr(0) == '(') parseToTree(expr.trim.substring(1, expr.length - 1))
      else new QueryPhysicalPlan.InputNode(expr)
    } else {
      expr(opLoc) match {
        case 'J' => new QueryPhysicalPlan.BinaryOpNode(parseToTree(expr.substring(0, opLoc)),
          parseToTree(expr.substring(opLoc + 1)))
      }
    }
  }

  def apply(vars: mutable.LinkedHashMap[String, DataFrame]): DataFrame = {
    root(vars)
  }
}


object QueryPhysicalPlan {
  def apply(expr: String): QueryPhysicalPlan = new QueryPhysicalPlan(expr)

  trait Node {
    def apply(vars: mutable.LinkedHashMap[String, DataFrame]): DataFrame
  }

  class InputNode(name: String) extends Node {
    override def apply(vars: mutable.LinkedHashMap[String, DataFrame]): DataFrame = vars(name)
  }

  class BinaryOpNode(left: Node, right: Node) extends Node {
    override def apply(vars: mutable.LinkedHashMap[String, DataFrame]): DataFrame
    = joinSP(left(vars), right(vars))

    def joinSP(dfLeft: DataFrame, dfRight: DataFrame): DataFrame = {
      val columnsName1 = dfLeft.columns
      val columnsName2 = dfRight.columns
      val joinKey = new mutable.ArrayBuffer[String]

      val joinType: String = "inner"

      for {i <- columnsName2 if columnsName1 contains i} joinKey.append(i)

      dfLeft.join(dfRight, joinKey, joinType)
    }
  }

}
