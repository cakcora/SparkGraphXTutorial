package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, Graph, GraphLoader, VertexId}

/**
  * Created by cxa123230 on 10/29/2016.
  */
object GraphXTutorial7DegreeAndNeighbors {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
//    val graph = GraphLoader.edgeListFile(sc,"src/main/resources/dblpgraph.txt")
    val graph: Graph[Double, Int] =
    //      GraphGenerators.logNormalGraph(sc, numVertices = 4).mapVertices( (id, _) => id.toDouble )
      new GraphXTutorial0Builder().createIntToyGraph(sc)
    // Compute the max degrees
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)
    println("maxIn:"+maxInDegree+" maxOut:"+maxOutDegree+" max:"+maxDegrees)

    println("Collect neighbor ids and show them:")
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
    neighbors.foreach(a=>{
      val str2 = a._2.toList.toString()
      println(a._1+" "+ str2);

    })

  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
}
