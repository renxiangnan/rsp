package sparkstreaming.sparqlquery

/**
  * Created by xiangnanren on 25/05/16.
  */
object QueryInitializer {
  val choice: String = "1"

  def initializeQueryStr: String = choice match {

    case "0" =>
      "select ?s ?o2" +
        "{" +
        "?s <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o ; " +
        "   <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o1 . " +
        "?o1 <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o2 ." +
        "FILTER(?s = <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ) }"

    case "1" =>
      "select ?s ?o1" +
        " { " +
        " ?s  <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o ." +
        " ?o <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o1 ; " +
        " } "

    case "2" =>
      "select ?s ?o1 ?o2 ?o3 ?o4 ?o5 ?o6" +
        //          "select ?s ?o6" +
        " { " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ." +
        " " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 ; " +
        "    <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " } "

    case "3" =>
      "select ?s ?o6" +
        " { " +
        " ?o2 <http://data.nasa.gov/qudt/owl/qudt/numericValue> ?o6 . " +
        " ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
        "    <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 . " +
        " ?o2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o3 . " +
        " ?s  <http://purl.oclc.org/NET/ssnx/ssn/hasValue> ?o2 ." +
        " ?o2 <http://purl.oclc.org/NET/ssnx/ssn/startTime> ?o4 ; " +
        "     <http://data.nasa.gov/qudt/owl/qudt/unit> ?o5 . " +
        " } "
  }
}


