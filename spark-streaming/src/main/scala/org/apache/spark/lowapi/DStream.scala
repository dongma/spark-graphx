package org.apache.spark.lowapi

import org.apache.spark.common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
 * Low level API DStream
 *
 * @author Sam Ma
 * @date 2024/06/08
 */
object DStream {

  val spark = SparkSession.builder().appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark Streaming Context = entry point to the DStream API
    - needs the spark context
    - a duration = batch interval
   */
  val scc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
   - define input sources by creating DStreams
   - define transformations on DStreams
   - start ALL computations with scc.start()
     - no more computations can be added
   - await termination, or stop the computation
     - you cannot restart a computation
   */
  def readFromSocket() = {
    val socketStream: DStream[String] = scc.socketTextStream("localhost", 12345)
    // transformation lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))
    // action
    wordsStream.print()

    scc.start()
    scc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(5000)
        val path = "example-data/rtjvm/stocks"
        val dir = new File(path) // directory where I will store a new file
        val nFiles = dir.listFiles().length
        val newFile = new File(s"$path/newStocks$nFiles.csv")
        newFile.createNewFile()

        val writer = new FileWriter(newFile)
        writer.write(
         """|MSFT,Jan 1 2000,39.81
            |MSFT,Feb 1 2000,36.35
            |MSFT,Mar 1 2000,43.22
            |MSFT,Apr 1 2000,28.37
            |MSFT,May 1 2000,25.45
            |MSFT,Jun 1 2000,32.54""".stripMargin)
        writer.close()
      }
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "example-data/rtjvm/stocks"
    /*
      ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream: DStream[String] = scc.textFileStream(stocksFilePath)

    // transformation
    val dateFormat = new SimpleDateFormat("MMM d yyyy", Locale.US)
    val stockStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble
      Stock(company, date, price)
    }
    // action
    stockStream.print()
    // start the computations
    scc.start()
    scc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromFile()
  }

}
