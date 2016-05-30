package sparkstreaming.consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Created by xiangnanren on 26/05/16.
  */
object SparkContextResolver {

  def apply: StreamingContext = {

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    Logger.getRootLogger.setLevel(Level.WARN)

    new StreamingContext(sparkConf, Duration(5000))
  }
}
