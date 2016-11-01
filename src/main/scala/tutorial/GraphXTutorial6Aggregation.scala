package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexRDD}

/**
  * Created by cxa123230 on 10/28/2016.
  */
object GraphXTutorial6Aggregation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph: Graph[Double, Int] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 4).mapVertices( (id, _) => id.toDouble )
      new GraphXTutorial0Builder().createIntToyGraph(sc)
    // Compute the number of older followers and their total age
    println("vertices are:")
    graph.vertices.foreach(println)
    println("edges are:")
    graph.edges.foreach(println)
    println
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function

        if (triplet.srcAttr > triplet.dstAttr) {
          // in our case, if the source attr is bigger than the dest attr
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      //we have multiple (1, triplet.srcAttr) values.
      //for example, a=(1,7) coming from edge Edge(7,3,21), and b=(1,5) coming from Edge(5,3,15)
      // at the third vertex. The 3d vertex will add them
      //and report (3,(2,12.0))
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    olderFollowers.foreach(println)
    println
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues( (id, value) =>
      value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println)
    println("Results show that the third vertex had two older neighbors 5, and 7. their average is 6.0.")
    sc.stop()
  }
}
