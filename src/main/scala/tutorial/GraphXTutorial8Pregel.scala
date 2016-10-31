package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.SparkSession
/**
  * Created by cxa123230 on 10/31/2016.
  */
object GraphXTutorial8Pregel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
//    GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    new GraphXTutorial0Builder().createLongToyGraph(sc)
    val sourceId: VertexId = 4 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    println("Only one vertex should be printed:")
    initialGraph.vertices.filter(a=>a._1==sourceId).foreach(println)
    println("- - - - - - - -")
    val sssp = initialGraph.pregel(Double.PositiveInfinity/*starting message to vertices*/)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program, update distance
      triplet => {  // Send Message
        //
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr)) // found smaller distance, continue
        } else {
          Iterator.empty// no change for the distance, can stop
        }
      },
      (a, b) => math.min(a, b) // Merge Message, when receiving multiple distances, pick the smallest
    )
    println("distances of nodes to node "+sourceId+
    "node 0 is not connected to the graph, its distance is infinity")

    println(sssp.vertices.collect.mkString("\n"))
  }
}
