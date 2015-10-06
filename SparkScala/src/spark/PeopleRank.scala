package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

object PeopleRank {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("PeopleRank")
    val sc = new SparkContext(conf)

    //读入数据文件
    val articles = sc.textFile("hdfs://hadoop:9000/user/hdfs/recommend/vertices") //不需要顶点信息直接搞个空文件就行了
    val links = sc.textFile("hdfs://hadoop:9000/user/hdfs/recommend/userbase/step3_3_spark")

    //装载顶点和边
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }

    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }

    //cache操作
    //val graph = Graph(vertices, edges, "").persist(StorageLevel.MEMORY_ONLY_SER)
    val graph = Graph(vertices , edges, "").persist()
    //graph.unpersistVertices(false)

    //测试
    println("**********************************************************")
    println("获取5个triplet信息")
    println("**********************************************************")
    graph.triplets.take(5).foreach(println(_))

    //pageRank算法里面的时候使用了cache()，故前面persist的时候只能使用MEMORY_ONLY
    println("**********************************************************")
    println("PageRank计算，获取最有价值的数据")
    println("**********************************************************")
    val prGraph = graph.pageRank(0.001).cache()

    println("**********************************************************")
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    titleAndPrGraph.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)  //按entry结构中的entry._2._1（pr值）排序
    }.foreach(t => println(t._1 + ":" + t._2._2 + ": " + t._2._1))  //titleAndPrGraph.vertices的结构【VerID、(Double,String)】

    val rdd = prGraph.vertices.map( t =>  t.toString().substring(1,t.toString().length()-1) )
    rdd.saveAsTextFile("hdfs://hadoop:9000/user/hdfs/recommend/userbase/step3_3")
    sc.stop()
  }
}

