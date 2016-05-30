package sparkstreaming.consumer

/**
  * Created by xiangnanren on 25/05/16.
  */

import sparkstreaming.parser._
import sparkstreaming.sparqlquery.QueryInitializer



object RDFReceiver {

  val queryStr: String = QueryInitializer.initializeQueryStr

  val visitor = SparqlQueryResolver(queryStr).visitor

  val collector = SparqlOpCollector(visitor)

  val lp = OptimizedPlan(collector).createLogicalPlan

  val pp = QueryPhysicalPlan(lp)


  def main(args: Array[String]) {

    val ssc = SparkContextResolver.apply

    ssc.checkpoint("/Users/xiangnanren/IDEAWorkspace/" +
      "spark/rsp/src/main/resources/checkpoint")

    val cc = new ConsumerConfigurator

    display()

    val dstream = DStreamCreator(ssc, cc).dstream


    //    dstream.window(Seconds(10), Seconds(5)).foreachRDD { rdd =>
    dstream.foreachRDD { rdd =>

//      val tStart: Long = System.nanoTime()

      val wdf0 = SQLContextResolver(rdd).df
//      wdf0.show(30,false:Boolean)

      val wdf = PredicatePusher(collector, wdf0).predicatePushDown
//      wdf.show(30, false: Boolean)

      val resSP = SPExecutor(collector, wdf).opDataFrameMap

      val interRes = pp(resSP)

      val countRes = ResultFormatter(collector, interRes).display.count()
      println(countRes)


//      val tEnd: Long = System.nanoTime()
//
//      println(s"The number of results: $countRes" + (tEnd-tStart))

    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def display():Unit = {
    println("varWeightMap: " + collector.varWeightMap + "\n")
    println("opProjects: " + collector.opProjects + "\n")
    println("opSPMap: " + collector.opSPMap + "\n")
    println("predicates: " + collector.predicates + "\n")
    println("optimizedPlan: " + lp + "\n")
  }




}









