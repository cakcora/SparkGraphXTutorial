package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by cxa123230 on 10/27/2016.
  */
object GraphXTutorial4GraphStructures {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[(String, String), String] = new GraphXTutorial0Builder().createToyGraph(sc)
    graph.triplets.map(
      triplet => triplet.srcAttr._1 +"("+triplet.srcAttr._2+") is the " + triplet.attr + " of " + triplet.dstAttr._1+"("+triplet.dstAttr._2+")"
    ).collect.foreach(println(_))

    // Remove people that are not professors as well as the edges to connected to them
    // vpred is a keyword that stands for vertex predicate. It is used to select vertices.
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 == "prof")
    println("Removing non prof vertices, the graph has vertices:")
    validGraph.vertices.collect.foreach(println(_))

    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1+" they are both "+triplet.srcAttr._2+"s."
    ).collect.foreach(println(_))

    println("Now, we will write an edge predicate. Only the colleague edges will remain in the graph.")
    val validGraph2 = graph.subgraph(epred => epred.attr=="colleague")
    validGraph2.edges.collect.foreach(println)

    println("Masking the original graph with a small and different graph")
    val maskGraph = maskedGraph(sc)
    val newGraph = graph.mask(maskGraph)
    newGraph.vertices.foreach(println)
    newGraph.edges.foreach(println)
    println("What happens if graph order changes?")
    val newGraph2 = maskGraph.mask(graph)
    newGraph2.vertices.foreach(println)
    newGraph2.edges.foreach(println)
    sc.stop()
  }

  def maskedGraph(sc:SparkContext): Graph[(String, String), String] ={
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((100L, ("cuneyt", "procrastinator")), (7L, ("jgonzal", "postdoc"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),Edge(100L, 7L, "makesFunOf")))
    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph
  }
}
