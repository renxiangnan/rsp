package sparkstreaming.parser

import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 24/05/16.
  */
class PredicatePusher(collector: SparqlOpCollector, wdf: DataFrame)
  extends OpExecutor {

  def predicatePushDown: DataFrame = {
    var condition: Option[String] = None

    for (predicate <- collector.predicates) {
      if (condition.isEmpty) condition =
        Some(wdf.columns(1) + " = \"" + predicate.getValue + "\"")
      else condition =
        condition.map(x => x + " or " + wdf.columns(1) + " = \"" + predicate.getValue + "\"")
    }
    wdf.filter(condition.get)
  }
}


object PredicatePusher {
  def apply(collector: SparqlOpCollector, wdf: DataFrame): PredicatePusher
  = new PredicatePusher(collector, wdf)
}