package sparkstreaming.parser

import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 24/05/16.
  */
class ResultFormatter(collector: SparqlOpCollector, interRes: DataFrame)
  extends OpExecutor {
  def display: DataFrame = {
    if (collector.opProjects.nonEmpty) selectType
    else selectType

  }

  def selectType: DataFrame = {
    val selectRes1 = collector.opProjects.map(x => x.getTargetName)
    val selectRes0 = selectRes1.head

    selectRes1.remove(0)

    interRes.select(selectRes0, selectRes1: _*)
  }
}


object ResultFormatter {
  def apply(collector: SparqlOpCollector, interRes: DataFrame): ResultFormatter
  = new ResultFormatter(collector, interRes)
}