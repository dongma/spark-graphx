import org.apache.spark.graphx._

// 已经将cit-HepTh.txt放入到spark的bin目录
var graph = GraphLoader.edgeListFile(sc, "cit-HepTh.txt");
// 对于PageRank设置0.0001用于平衡计算速度和准确度
var pageRank = graph.pageRank(0.0001).vertices
// 获取查询出来的前10个pageRank计算结果
pageRank.take(10);