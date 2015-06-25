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

    val rawRDD = sc.textFile(args(0)).
      map(row => {
      row.split("\t")
        .map(_.trim())
        .filter(_.length == 3)
        .filter(x => x(0).toLong >= 1080 && x(1).toLong >= 1080)
    }).map(x => (x(0), x(1)))

    val reversedRDD = rawRDD.map(x => (x._2, x._1))

    val allRDD = reversedRDD.join(rawRDD).filter(x => x._1 == x._2._2).map(x => (x._1, x._2._1))

    val edgeRDD = allRDD
      .map(x => new Edge(x._1.toLong, x._2.toLong, 1L))

    val graph = Graph.fromEdges(edgeRDD, None)

    val minProgress = 2000
    val progressCounter = 1
    val outputDir = "."
    val runner = new HDFSLouvainRunner(minProgress, progressCounter, outputDir)
    runner.run(sc, graph)

  }
}
