name := "rsp"

version := "1.0"

scalaVersion := "2.10.5"

//Spark dependencies
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.6.0"

//sesame dependencies
libraryDependencies += "org.openrdf.sesame" % "sesame-model" % "4.1.2"
libraryDependencies += "org.openrdf.sesame" % "sesame-query" % "4.1.2"
libraryDependencies += "org.openrdf.sesame" % "sesame-queryparser-sparql" % "4.1.2"

//Amazon AWS dependecies
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.10" % "1.6.1"
