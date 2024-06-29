package org.apache.spark.integrations

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.typesafe.config.ConfigFactory
import org.apache.spark.common.{Car, carSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Spark Streaming整合Akka Stream
 *
 * @author Sam Ma
 */
object Akka {

  val spark = SparkSession.builder()
    .appName("Akka System")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // foreachBatch
  // receiving system is an another JVM，问题：java.lang.ClassNotFoundException: remote
  def writeCarsToAkka(): Unit = {
    val carsDS = spark.readStream
      .schema(carSchema)
      .json("example-data/rtjvm/cars").as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
          batch.foreachPartition { cars: Iterator[Car] =>
            // this code is run by a single executor
            val system = ActorSystem(s"SourceSystem$batchId",
              ConfigFactory.load("akkaconfig/remoteActors"))
            val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")
            // send all the data
            cars.foreach(car => entryPoint ! car)
          }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToAkka()
  }

}

object ReceiveSystem {
  implicit val actorSystem = ActorSystem("ReceiverSystem",
    ConfigFactory.load("akkaconfig/remoteActors").getConfig("remoteSystem"))
  implicit val actorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive: Receive = {
      case m => log.info(m.toString)
    }
  }

  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {
      case m =>
        log.info(s"Received $m")
        destination != m
    }
  }

  def main(args: Array[String]): Unit = {
    val source = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Car](println)
    val runnableGraph = source.to(sink)
    val destination: ActorRef = runnableGraph.run()

    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination))
  }

}
