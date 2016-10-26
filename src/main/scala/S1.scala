import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by cxa123230 on 10/22/2016.
  */
object S1 {


  def wawe(graph: Graph[Int, Int]): Unit ={
    val fg = graph.collectNeighborIds(EdgeDirection.Either)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc,"C:/Projects/DisagioData/dblpgraph.txt")
    val edgeCount = graph.numEdges
    val vertexCount = graph.numVertices
    println(vertexCount+" vertices "+edgeCount+" edges")


      wawe(graph);


  }
}
