package sparkstreaming.consumer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by xiangnanren on 26/05/16.
  */
class SQLContextResolver(rdd: RDD[scala.collection.mutable.Set[Stream[String]]]) {

  val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)

  import sqlContext.implicits._

  val df = rdd.flatMap(x => x).map(x => (x.head, x(1), x(2))).
    toDF("sDefault", "pDefault", "oDefault")
}

object SQLContextResolver {
  def apply(rdd: RDD[mutable.Set[Stream[String]]]): SQLContextResolver
  = new SQLContextResolver(rdd)
}