 package tutorial

import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
  * Created by cxa123230 on 10/26/2016.
  */
object G1GraphOps {

  def main(args: Array[String]): Unit = {
    val graph:Graph[(String, String), String] = new G1Create().createToyGraph
    val g2:Graph[(String, String), Int] = graph.mapTriplets(triplet => triplet.srcAttr._1.length)
    val t1: Array[Edge[Int]] = g2.edges.take(1)
    val t2= g2.vertices.take(1)
    //.mapVertices((id, _) => 4.0)
    val name = t1(0)
    val functi = t1(0)
    println(name+" "+functi+" "+t2(0))
    graph.vertices.foreach(println)
    println("******************")
    g2.vertices.foreach(println)
    println("-------------------")
    graph.edges.foreach(println)
    println("******************")
    g2.edges.foreach(println)

  }
}
