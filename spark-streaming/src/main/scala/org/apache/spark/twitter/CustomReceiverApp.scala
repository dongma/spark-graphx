package org.apache.spark.twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.Socket
import scala.concurrent.{Future, Promise}
import scala.io.Source

/**
 * 自定义消息接收App
 *
 * @author Sam Ma
 * @date 2024/06/28
 */
class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  // called asynchronously
  override def onStart(): Unit = {
    val socket = new Socket(host, port)
    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store makes this string available in Spark
    }
    socketPromise.success(socket)
  }
  // called asynchronously
  override def onStop(): Unit = {
    socketFuture.foreach(socket => socket.close())
  }
}

object CustomReceiverApp {

  val spark = SparkSession.builder()
    .appName("Custom Receiver App")
    .master("local[*]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {
    val dataStream: DStream[String] = ssc.receiverStream(new CustomSocketReceiver("localhost", 12345))
    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
