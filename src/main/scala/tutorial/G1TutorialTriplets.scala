package tutorial

import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cxa123230 on 10/26/2016.
  */
object G1TutorialTriplets {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    val graph = Graph(users, relationships)
    val exp: RDD[String] =
    graph.triplets.map(triplet =>
    "For the first vertex of the edge, srcAttr._1 is "+triplet.srcAttr._1 + ", srcAttr._2 is " + triplet.srcAttr._2 +
    "\n\tedge attribute is  " + triplet.attr+
    "\n\t\tFor the second vertex of the edge, dsAttr._1 is "+triplet.dstAttr._1 + ", dstAttr._2 is " + triplet.dstAttr._2 )

    val facts: RDD[String] =
    graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    exp.collect.foreach(println(_))
    facts.collect.foreach(println(_))
  }
}
