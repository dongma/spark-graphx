import org.apache.spark.graphx._

// 已经将cit-HepTh.txt放入到spark的bin目录
var graph = GraphLoader.edgeListFile(sc, "cit-HepTh.txt");
// graph.inDegrees返回为Tuple<vertexId, inDegree>,在匿名韩式中_2指代的Tuple中的second value
graph.inDegrees.reduce((a, b) => if(a._2 > b._2) a else b);
// 计算得到的是出度最多的<vertexId, degrees>
// res0: (org.apache.spark.graphx.VertexId, Int) = (9711200,2414)