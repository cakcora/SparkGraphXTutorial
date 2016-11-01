import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cxa123230 on 10/26/2016.
  */
class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty

object GraphXTutorial1Basic {

  // The graph might then have the type:

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
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships,defaultUser)
    val co = graph.numVertices
    val ed = graph.numEdges
    val posCount = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    val edCount = graph.edges.filter(e => e.srcId > e.dstId).count
    val altedCount= graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    val adCount = graph.edges.filter(e => e.attr=="advisor").count
    val d:VertexId =7L
    val neighborsOf7L = graph.edges.filter(e => e.otherVertexId(e.srcId)==d).count

    println(co + " "+ ed+" "+posCount+". "+edCount+" is the same as "+altedCount+". "+adCount)
    println("7L has "+neighborsOf7L+" neighbors")

    println(altedCount)
  }

}
