package sparkstreaming.parser

/**
  * Created by xiangnanren on 24/05/16.
  */
abstract class QueryLogicalPlan(collector: SparqlOpCollector) {

  def createLogicalPlan: String

}
