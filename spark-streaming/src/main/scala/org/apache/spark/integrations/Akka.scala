package org.apache.spark.integrations

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

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

  // foreachBatch receiving system is an another JVM
  def main(args: Array[String]): Unit = {

  }

}

object ReceiveSystem {
  val actorSystem = ActorSystem("ReceiverSystem",
    ConfigFactory.load("akkaconfig/remoteActors.conf").getConfig("remoteSystem"))

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
    val destination = actorSystem.actorOf(Props[Destination], "destination")
    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination))
    // TODO:
  }

}
