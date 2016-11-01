import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, _}
import org.apache.spark.rdd.RDD
/**
  * Created by cxa123230 on 10/26/2016.
  * This helper class creates graph examples that are used in other classes.
  * This class does not contain any runnable code.
  * You may continue to study Graph1Basic.scala
  */
class GraphX0Builder {

  def createToyGraph(sc:SparkContext): Graph[(String, String), String] ={


    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships,defaultUser)
    graph
  }
  def createDoubleToyGraph(sc:SparkContext): Graph[Double, Double] ={
    // Create an RDD for the vertices
    val users: RDD[(Long,Double )] =
    sc.parallelize(Array((3L, (1.0)), (7L, (2.0)),
      (5L, (3.0)), (2L, (4.0))))
    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(Edge(3L, 7L, 1.0),    Edge(5L, 3L, 2.0),
      Edge(2L, 5L, 3.0), Edge(5L, 7L, 4.0)))
    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph
  }
  def createIntToyGraph(sc:SparkContext): Graph[Double, Int] ={
    // Create an RDD for the vertices
    val users: RDD[(Long,Double )] =
    sc.parallelize(Array((3L, (3.0)), (7L, (7.0)),
      (5L, (5.0)), (2L, (2.0))))
    // Create an RDD for edges
    val relationships: RDD[Edge[Int]] =
    sc.parallelize(Array(Edge(7L, 3L, 21),    Edge(5L, 3L, 15),
      Edge(2L, 5L, 10), Edge(7L, 5L, 35)))
    // Build the initial Graph
    val graph = Graph(users, relationships)
    graph
  }
  def createLongToyGraph(sc:SparkContext): Graph[Long, Double] ={
    // Create an RDD for the vertices
     val dummyInt:Long = 4
    val users: RDD[(Long,Long )] =
    sc.parallelize(Array((0L, dummyInt), (1L, dummyInt),
      (2L, dummyInt), (3L, dummyInt),(4L, dummyInt),(5L,dummyInt)))
    // Create an RDD for edges
    val dummyDouble:Double = 1.0
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(    Edge(4L, 1L, dummyDouble),
      Edge(4L, 3L, dummyDouble), Edge(3L, 2L, dummyDouble), Edge(1L, 2L, dummyDouble), Edge(2L, 5L, dummyDouble)))
    // Build the initial Graph
     Graph(users, relationships)
  }
}
