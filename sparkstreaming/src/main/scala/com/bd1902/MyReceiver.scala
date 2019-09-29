package com.bd1902

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 自定义的数据源类  用来接收socket消息
  * @param host
  * @param port
  */
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  def   receive(): Unit ={
    var message:String = null
    var socker:Socket=null

    try{
      socker = new Socket(host,port)//说明可以连接上服务器
      //读取消息  需要输入流
      var  reader = new BufferedReader(new InputStreamReader(socker.getInputStream,StandardCharsets.UTF_8))

      while((message = reader.readLine()) != null){
        //发出去  放到batch
        store(message)
      }
      reader.close()
      socker.close()

      //可以重新开启
    }

  }

  override def onStart(): Unit = {
    new Thread("自定义的线程类"){
      override def run(): Unit = {
        receive();//开线程接收消息
      }

    }.start()
  }

  override def onStop(): Unit = {

  }

}
