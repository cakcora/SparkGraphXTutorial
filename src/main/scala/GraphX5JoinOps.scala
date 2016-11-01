import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cxa123230 on 10/27/2016.
  */
object GraphX5JoinOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[Double, Double] = new GraphX0Builder().createDoubleToyGraph(sc)
    println("graph vertices:")
    graph.vertices.foreach(println)
    println("graph edges:")
    graph.edges.foreach(println)
    println("we have new costs coming from external data. But  costs are not unique, 3L has 10.0 and 1.0 in two different records.")
    val nonUnique: RDD[(Long, Double)]=sc.parallelize(Array((3L,10.0),(3L,1.0),(7L,2.0),(7L,6.0),(1L,8.0),(1L,9.0)))
    val uniqueCosts: VertexRDD[Double] =
      graph.vertices.aggregateUsingIndex(nonUnique, (a,b) => (a+b))
    println("now, getting unique costs")
    uniqueCosts.foreach(println)
    println("Above, values of vertices were aggregated, so that vertex 3L got 10.0+1.0->11.0." +
      "\nnote that 1L was ignored even though it had two values 8 and 9 because it is not a vertex in the graph.")

    println
    println("Now let's add values from unique costs (3->11.0 and 7->8.0) to the costs that already exist at vertices. " +
      "(3->1.0),(2->4.0),(7->2.0),(5->3.0)")
    val joinedGraph:Graph[Double, Double] = graph.joinVertices(uniqueCosts)(
      (id, v1,v2) =>  v1+v2 )

    joinedGraph.vertices.foreach(println)
    println("outerjoinvertices can change the vertex property type")
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    val degreeGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 // No outDegree means zero outDegree
      }
    }
    degreeGraph.vertices.foreach(println)
    sc.stop()
  }

}
