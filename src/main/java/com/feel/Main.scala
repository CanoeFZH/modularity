package com.feel

/**
 * Created by canoe on 6/24/15.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object Main {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf)

    val rawRDD = sc.textFile(args(0))
      .distinct(10)
      .map(row =>
      row.split("\t")
      ).filter(_.length == 3)
      .filter(x => x(0).toLong >= 1080 && x(1).toLong >= 1080)

    val rdd1 = rawRDD.map(x => (x(0) + "\t" + x(1), "A"))

    val rdd2 = rawRDD.map(x => (x(1) + "\t" + x(0), "B"))

    val allRDD = rdd1.join(rdd2)

    allRDD.map(x => x._1).saveAsTextFile(args(1))

    val edgeRDD = allRDD
      .distinct(10)
      .map(x => x._1.split("\t"))
      .map(x => new Edge(x(0).toLong, x(1).toLong, 1L))

    val graph = Graph.fromEdges(edgeRDD, None)

    val minProgress = 2000
    val progressCounter = 1
    val outputDir = "./"
    val runner = new HDFSLouvainRunner(minProgress, progressCounter, outputDir)
    runner.run(sc, graph)

  }
}
