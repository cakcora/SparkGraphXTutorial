package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by cxa123230 on 10/27/2016.
  */
object GraphXTutorial5JoinOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[Long, Double] = new GraphXTutorial0Builder().createNumericToyGraph(sc)
    graph.vertices.foreach(println)
    graph.edges.foreach(println)
    println
    val nonUnique: RDD[(Long, Long)]=sc.parallelize(Array((3L,10L),(3L,1L),(7L,2L),(7L,6L),(1L,8L),(1L,9L)))
    val uniqueCosts: VertexRDD[Long] =
      graph.vertices.aggregateUsingIndex(nonUnique, (a,b) => (a+b))

    uniqueCosts.foreach(println)
    println("Above, values of vertices were aggregated, so that vertex 3L got 10L+1L->11L." +
      "\nnote that 1L was ignored even though it had two values 8 and 9 because it is not a vertex in the graph.")

    println
    val joinedGraph:Graph[VertexId, Double] = graph.joinVertices(uniqueCosts)(
      (id, v1,v2) =>  v1)

    joinedGraph.vertices.foreach(println)
    joinedGraph.edges.foreach(println)
  }

}
