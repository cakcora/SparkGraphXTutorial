import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 10/31/2016.
  */
object GraphX9Builders {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)

    println("The most basic graph can be created by giving an array of vertices" +
      "and and array of edges. Check Graph(users, relationships) in GraphX0Builder.scala:")
    val primitiveGraph = new GraphX0Builder().createIntToyGraph(sc);

    println("Another option is to create a graph from edges:")
    val arr: RDD[Edge[Int]] = sc.parallelize(Array(Edge(7L, 3L, 21), Edge(5L, 3L, 15),
      Edge(2L, 5L, 10), Edge(7L, 5L, 35)))
    val defaultVertexAttribute: Long = 1
    val rdGraph= Graph.fromEdges(arr,defaultValue=defaultVertexAttribute)
    println("Here are the created vertices.")
    rdGraph.vertices.foreach(println)
    println("We can also create a graph from tuples of vertices: ")
    val re:RDD[(Long, Long)] = sc.parallelize(Array((7L, 3L), (5L, 3L),
      (2L, 5L), (7L, 5L),(7L, 5L)))
    val tupleGraph = Graph.fromEdgeTuples(re,defaultValue = 1,
      uniqueEdges = Some(PartitionStrategy.RandomVertexCut))
    tupleGraph.edges.foreach(println)
    println("Note that the duplicate edge 7->5 is indicated with Edge(7,5,2)")

    println("The last method is to use the graph loader class and give it an edge list in a file:")
    val graph = GraphLoader.edgeListFile(sc,"src/main/resources/dblpgraph.txt")

    println("Graph has "+graph.numVertices+" vertices and "+graph.numEdges+" edges")
    spark.stop()
  }
}
