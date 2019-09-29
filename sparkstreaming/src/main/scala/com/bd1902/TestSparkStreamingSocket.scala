package com.bd1902

import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TestSparkStreamingSocket {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("socket").setMaster("local[2]")
    var sparkStreaming = new StreamingContext(conf,Seconds(5))


    //当做rdd进行处理
    var ds = sparkStreaming.socketTextStream("hadoop101",9999)
    ds.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()

    sparkStreaming.start()

   // sparkStreaming.awaitTermination()//阻塞方法
    sparkStreaming.awaitTerminationOrTimeout(10000)
  }

}
