package com.bd1902

import org.apache.spark.{SparkConf, SparkContext}

object TestPartion {


  def partitionFunctionWithIndex(index:Int,iter:Iterator[Int]): Iterator[(Int,Int)] ={
    var list = List[(Int,Int)]()
    while(iter.hasNext){
      var value = iter.next()
      list = (index,value)::list
    }
    list.iterator
  }

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local").setAppName("test")
    var sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,0),3)

    rdd1.mapPartitionsWithIndex(partitionFunctionWithIndex).foreach(println)
    println("=======")
   var rdd2 =  rdd1.repartition(4)
    rdd2.mapPartitionsWithIndex(partitionFunctionWithIndex).foreach(println)
    sc.stop()
  }

}
