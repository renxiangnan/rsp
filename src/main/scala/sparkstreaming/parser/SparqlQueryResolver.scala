package sparkstreaming.parser

import org.openrdf.query.parser.ParsedQuery
import org.openrdf.query.parser.sparql.SPARQLParser

/**
  * Created by xiangnanren on 12/05/16.
  */
class SparqlQueryResolver(queryStr: String) {

  private val parser: SPARQLParser = new SPARQLParser
  private val query: ParsedQuery = parser.parseQuery(queryStr, null)
  val visitor: SparqlOpVisitor = SparqlOpVisitor("TEST")

  query.getTupleExpr.visit(visitor)

}

object SparqlQueryResolver {
  def apply(queryStr: String) = new SparqlQueryResolver(queryStr: String)
}
