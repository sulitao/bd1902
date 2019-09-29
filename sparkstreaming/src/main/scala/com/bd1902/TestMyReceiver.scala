package com.bd1902

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestMyReceiver {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("socket").setMaster("local[2]")
    var sparkStreaming = new StreamingContext(conf,Seconds(20))

    //使用自定义数据源处理消息
    var ds = sparkStreaming.receiverStream(new MyReceiver("hadoop102",8888))
    ds.flatMap(_.split(",")).print(100)

    sparkStreaming.start()
    sparkStreaming.awaitTermination()//阻塞的

  }

}
