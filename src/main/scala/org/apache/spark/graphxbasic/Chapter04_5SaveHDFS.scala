package org.apache.spark.graphxbasic

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object Chapter04_5SaveHDFS {

  def main(args: Array[String]): Unit = {
    // 使用Hadoop API将Graph序列化后的文件以单个文件的形式保存到HDFS中
    val configuration = new org.apache.hadoop.conf.Configuration()

    /**
      * hadoop command line:
      * hadoop fs -getmerge /user/cloudera/myGraphVertices myGraphVerticesFi
      */
    configuration.set("fs.defaultFS", "hdfs://localhost")
    val fileSystem = FileSystem.get(configuration)

    /**
      * TODO:
      * Exception in thread "main" java.net.ConnectException: Call From DESKTOP-V261DB3/10.0.75.1 to localhost:8020
      * failed on connection exception: java.net.ConnectException:
      */
    FileUtil.copyMerge(fileSystem, new Path("myGraphEdges"), fileSystem, new Path("myGraphVertices"),
      false, configuration, null)
  }

}
