package org.apache.spark

import java.lang.management.ManagementFactory
import java.lang.reflect.Modifier


/**
 * spark streaming与apache kafka进行整合
 *
 * @author Sam Ma
 * @date 2022/10/27
 */
class KafkaStream {

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

}
