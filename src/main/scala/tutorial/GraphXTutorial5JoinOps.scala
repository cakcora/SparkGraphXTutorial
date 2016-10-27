package tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph

/**
  * Created by cxa123230 on 10/27/2016.
  */
object GraphXTutorial5JoinOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Disagio")

    val sc = new SparkContext(conf)
    Logger.getRootLogger().setLevel(Level.ERROR)
    val graph:Graph[(String, String), String] = new GraphXTutorial0Builder().createToyGraph(sc)
  }

}
