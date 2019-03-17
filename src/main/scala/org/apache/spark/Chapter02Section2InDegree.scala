import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Chapter02Section2InDegree {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("spark"));
    // 已经将cit-HepTh.txt放入到spark的bin目录
    var graph = GraphLoader.edgeListFile(sparkContext, "cit-HepTh.txt");
    // graph.inDegrees返回为Tuple<vertexId, inDegree>,在匿名函数中_2指代的Tuple中的second value
    graph.inDegrees.reduce((a, b) => if (a._2 > b._2) a else b);
    // 计算得到的是出度最多的<vertexId, degrees>
    // res0: (org.apache.spark.graphx.VertexId, Int) = (9711200,2414)
  }
}