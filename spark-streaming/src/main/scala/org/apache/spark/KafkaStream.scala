package org.apache.spark

import java.lang.management.ManagementFactory
import java.lang.reflect.Modifier
import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.spark.KafkaStream.{getCPUUsage, getMemoryUsage, getUsage}
import org.codehaus.jackson.map.ser.std.StringSerializer


/**
 * spark streaming与apache kafka进行整合
 *
 * @author Sam Ma
 * @date 2022/10/27
 */
class KafkaStream {

  /** 初始化kafka的连接配置，clientId作为参数用于连接kafka集群 */
  def initConfig(clientId: String): Properties = {
    val props = new Properties
    val brokerList = "localhost:9092"
    // 指定kafka集群broker列表，在其中指定prop参数配置
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    props
  }

  // 定义clientId与cpu、memory在kafka的topic列表
  val clientId = "usage.monitor.client"
  val cpuTopic = "cpu-monitor"
  val memoryTopic = "mem-monitor"
  // 定义属性，其中包括kafka集群信息、序列化方法，等等，同时定义kafkaProducer对象
  val props = initConfig(clientId)

  // 定义kafka producer对象，用于发送消息（回调函数，可暂时忽略）
  val producer = new KafkaProducer[String, String](props)
  val usageCallback = new Callback {
    override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit =
      println("exception:" + ex)
  }
  while (true) {
    // 调用之前定义的函数，获取cpu、memory的利用率
    val cpuUsage = getCPUUsage()
    val memoryUsage = getMemoryUsage()
    // 为cpu topic生成kafka消息，为memory topic生成kafka消息
    val cpuRecord = new ProducerRecord[String, String](cpuTopic, clientId, cpuUsage)
    val memoryRecord = new ProducerRecord[String, String](memoryTopic, clientId, memoryUsage)
    // 2.向kafka集群发送cpu、memory利用率的消息，设置发送间隔为2s
    producer.send(cpuRecord, usageCallback)
    producer.send(memoryRecord, usageCallback)
    Thread.sleep(2000)
  }

  def main(args: Array[String]): Unit = {

  }

}

object KafkaStream {

  /** 用反射获取操作系统Java Bean，并获取操作系统的cpu占用率 */
  def getUsage(methodName: String): Any = {
    val operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
    // 获取操作系统声明过的方法，并通过反射修改函数访问修饰符
    for (method <- operatingSystemMXBean.getClass.getDeclaredMethods) {
      method.setAccessible(true)
      // 调用并且执行方法，获取指定资源(cpu或内存)的利用率
      if (method.getName.startsWith(methodName) && Modifier.isPublic(method.getModifiers)) {
        return method.invoke(operatingSystemMXBean)
      }
    }
    throw new Exception(s"Can not reflect method: ${methodName}")
  }

  /** 获取系统的cpu资源利用率 */
  def getCPUUsage(): String = {
    var usage = 0.0
    try {
      usage = getUsage("getSystemCpuLoad").asInstanceOf[Double] * 100
    } catch {
      case e: Exception => throw e
    }
    usage.toString
  }

  /** 获取系统的内存占用率，还是通过getUsage("xxxx")方法来调用 */
  def getMemoryUsage(): String = {
    var freeMemory = 0L
    var totalMemory = 0L
    var usage = 0.0

    try {
      // 调用getUsage方法，传入相关内存参数，获取内存利用率
      freeMemory = getUsage("getFreePhysicalMemorySize").asInstanceOf[Long]
      totalMemory = getUsage("getTotalPhysicalMemorySize").asInstanceOf[Long]
      // 用总内存，减去空闲内存，获取当前内存使用量
      usage = (totalMemory - freeMemory.doubleValue) / totalMemory * 100
    } catch {
      case e: Exception => throw e
    }
    usage.toString
  }

}
