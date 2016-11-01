import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeRDD, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by cxa123230 on 10/31/2016.
  */
object GraphXTutorial10VertexEdgeRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger().setLevel(Level.ERROR)
    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 10L).map(id => (id, 1)))
    println("Actually setA: VertexRDD[Int] should be setA: VertexRDD[Double], but no worries" )
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 10L).flatMap(id => List((id, 1.0), (id, 2.0)))
    // There should be 20 entries in rddB,  because above we created 10 entries initially and then created two copies:(id, 1.0) and (id, 2.0)
    println(rddB.count+" entries in rddB")
    //aggregation of setA[Int] and setB[Double] through aggregateUsingIndex will not cause a problem.
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    // There should be 10 entries in setB, because vertexRDD prevents doubles.
    println(setB.count+" entries in rddB")
    // Joining A and B should now be fast!
    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
    setC.foreach(println)

    println("see that each vertex got a summed value of 4.0 because [(id, 1)+List((id, 1.0), (id, 2.0))]->(id,4.0)")

    println("Filter the vertex 1:")
    setA.filter(a=>a._1==1).foreach(println)
    println("SetB minus another RDD:")
    println(setB.minus(rddB).collect().length)

    val edgeList: RDD[Edge[Double]] = sc.parallelize(Array(Edge(1, 2, 1.0),Edge(1,2,1.0)))
    edgeList.foreach(println)
    val edges:EdgeRDD[Double] = EdgeRDD.fromEdges(edgeList)
    println("So :( it does not omit duplicates like VertexRDD. When reversed:")
    edges.reverse.foreach(println) // note that edges changed, they are inverted now.

//    edges.reverse.foreach(a=>a)//back to the original edges
    val otherEdges:EdgeRDD[Double] = EdgeRDD.fromEdges(sc.parallelize(Array(Edge(1, 2, 7.0),Edge(2,3,1.0))))

    val allEdges:EdgeRDD[Double] = edges.innerJoin(otherEdges)((edge1,edge2,edgeAtt1,edgeAtt2)=>if(edgeAtt1>edgeAtt2)edgeAtt1 else edgeAtt2)
    //choose the edge attribute that is bigger
    println("Inner join results are:")
    allEdges.foreach(println)
    spark.stop()

  }

}
