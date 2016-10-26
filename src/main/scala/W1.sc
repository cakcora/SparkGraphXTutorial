import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

import scala.io.Source
val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")
val sc = new SparkContext(conf)
val graph = GraphLoader.edgeListFile(sc,"C:/Projects/DisagioData/dblpgraph.txt")
graph.vertices.

