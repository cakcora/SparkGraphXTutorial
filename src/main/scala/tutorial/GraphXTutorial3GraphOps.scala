 package tutorial



import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}

/**
  * Created by cxa123230 on 10/26/2016.
  */
object GraphXTutorial3GraphOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[(String, String), String] = new GraphXTutorial0Builder().createToyGraph(sc)
    val graph2:Graph[(String, String), Int] = graph.mapTriplets(triplet => triplet.srcAttr._1.length)
    val t1: Array[Edge[Int]] = graph2.edges.take(1)

    println("original graph vertices are:")
    graph.vertices.foreach(println)
    println("New graph has these vertices:")
    graph2.vertices.foreach(println)
    println("Soo, mapTriplets() has not changes vertices at all. " )
    println()
      println("Lets look at edges. Here are the old graph edges:")
    graph.edges.foreach(println)
    println("Here, the new edges:")
    graph2.edges.foreach(println)
    println("Edge attribute has changed from collaboration type to length of the name of the source vertex " +
      "(that is, triplet.srcAttr._1.length)")
    println("Edge(3,7,collab)->Edge(3,7,4), because edge 3->7 has the source vertex 3 with a name rxin, |rxin|=4 ")
    println()
    val graph3:Graph[(String, String), Int] = graph.mapTriplets( triplet=> triplet.attr.length)
    graph3.edges.foreach(println)
    println("Note how Edge(2,5,colleague) in the original graph became Edge(2,5,9)")
    println()
    println("Now, a) we will change edge attribute of the graph from (String) to (Double) with mapTriplets().")
    println("b) we will change vertex attribute of the graph from (String, String) to (Double) with mapVertices().")
    val graph4: Graph[Double, Double] =
      graph.mapTriplets(triplet => 1.0*triplet.attr.length).mapVertices((_,_) => 1.0)
    graph4.edges.foreach(println)
    graph4.vertices.foreach(println)
    println("Notice how Edge(3,7,collab) became Edge(3,7,6.0) and vertex (2,(istoica,prof)) became (2,1.0)")
    println()
    val graph5:Graph[(String, String), Double]=
      graph.mapEdges(triplet => 1.0*triplet.attr.length)
    graph5.edges.foreach(println)
    println("Notice how Edge(3,7,collab) became Edge(3,7,6.0)")
    println("Summary:If you need to change vertex atts, use mapVertices, if you need to change edge attributes use mapEdges.")
    println("if you need to change edge attributes while using vertex attributes, use mapTriplets ")
    sc.stop()

  }
}
