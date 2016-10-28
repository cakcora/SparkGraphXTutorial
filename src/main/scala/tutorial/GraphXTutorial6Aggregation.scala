package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph

/**
  * Created by cxa123230 on 10/28/2016.
  */
class GraphXTutorial6Aggregation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[Double, Double] = new GraphXTutorial0Builder().createNumericToyGraph(sc)
  }
}
