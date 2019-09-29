package com.bd1902

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestSparkStreamingDirectory {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("socket").setMaster("local[2]")
    var sparkStreaming = new StreamingContext(conf,Seconds(5))

    var ds = sparkStreaming.textFileStream("hdfs://mycluster/bd1902/spark_data")

    ds.flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_).print()

    sparkStreaming.start()

    sparkStreaming.awaitTermination()

  }

}
