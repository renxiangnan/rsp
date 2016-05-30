package sparkstreaming.parser

import org.apache.spark.sql.DataFrame
import org.openrdf.query.algebra.StatementPattern

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 24/05/16.
  */
class SPExecutor(collector: SparqlOpCollector, wdf: DataFrame)
  extends OpExecutor {

  val opDataFrameMap = new mutable.LinkedHashMap[String, DataFrame]

  collector.opSPMap.foreach { case (key: StatementPattern, value: Int)
  => opDataFrameMap.put(value.toString, computeSP(key))
  }


  def selectSP(df: DataFrame): DataFrame = {
    val selectVar = new ArrayBuffer[String]

    collector.varWeightMap.foreach { case (vars, weight) =>
      if (df.columns.contains(vars)) selectVar += vars
    }

    val selectVar0 = selectVar.head
    selectVar.remove(0)

    df.select(selectVar0, selectVar: _*)
  }

  def computeSP(sp: StatementPattern): DataFrame = {
    val spS = sp.getSubjectVar
    val spP = sp.getPredicateVar
    val spO = sp.getObjectVar

    val df = (spS.hasValue, spP.hasValue, spO.hasValue) match {
      case (false, true, true) => wdf.withColumnRenamed("sDefault", spS.getName)
        .where(wdf("pDefault") <=> spP.getValue.stringValue() ).where(wdf("oDefault") <=> spO.getValue.stringValue())

      case (true, false, true) => wdf.withColumnRenamed("pDefault", spP.getName)
        .where(wdf("sDefault") <=> spS.getValue.stringValue()).where(wdf("oDefault") <=> spO.getValue.stringValue())

      case (true, true, false) => wdf.withColumnRenamed("sDefault", spO.getName)
        .where(wdf("sDefault") <=> spS.getValue.stringValue()).where(wdf("pDefault") <=> spP.getValue.stringValue())

      case (false, false, true) => wdf.withColumnRenamed("sDefault", spS.getName)
        .withColumnRenamed("pDefault", spP.getName)
        .where(wdf("oDefault") <=> spO.getValue.stringValue())

      case (false, true, false) => wdf.withColumnRenamed("sDefault", spS.getName)
        .withColumnRenamed("oDefault", spO.getName)
        .where(wdf("pDefault") <=> spP.getValue.stringValue())

      case (true, false, false) => wdf.withColumnRenamed("pDefault", spP.getName)
        .withColumnRenamed("oDefault", spO.getName)
        .where(wdf("sDefault") <=> spS.getValue.stringValue())

      case (false, false, false) => wdf.withColumnRenamed("sDefault", spS.getName)
        .withColumnRenamed("pDefault", spP.getName)
        .withColumnRenamed("oDefault", spO.getName)

      case (true, true, true) => wdf.where(wdf("sDefault") <=> spS.getValue.stringValue())
        .where(wdf("pDefault") <=> spP.getValue.stringValue())
        .where(wdf("oDefault") <=> spO.getValue.stringValue())
    }
    selectSP(df)
  }
}


object SPExecutor {
  def apply(collector: SparqlOpCollector, wdf: DataFrame): SPExecutor
  = new SPExecutor(collector, wdf)
}