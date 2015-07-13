package com.feel

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

case class recommend(user: String, candidates: Seq[String])

/**
 * Created by canoe on 7/10/15.
 */
object connectedComponent {
  def main(args: Array[String]) = {

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

    val edgeRDD = allRDD
      .distinct(10)
      .map(x => x._1.split("\t"))
      .map(x => new Edge(x(0).toLong, x(1).toLong, 1L))

    val graph = Graph.fromEdges(edgeRDD, None)
    val cc = graph.connectedComponents()

    val followRDD = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= 1080 && x(1).toInt >= 1080)
      .map(x => (x(1), x(0)))
      .reduceByKey((a, b) => a + "\t" + b)

    val recommendCandidates = cc.vertices.map(x => (x._2.toString, x._1.toString)).reduceByKey((a, b) => a + "\t" + b)
    .map(x => x._2.split("\t"))
    .flatMap(x => {
      val length = x.length
      if (length >= 1000) {
        val partitionSize = 100
        val partitionNumber = length / partitionSize
        val result = new Array[(String, String)](partitionNumber * partitionSize * partitionSize)
        for (i <- 0 until partitionNumber) {
          val resultStartIndex = i * partitionSize * partitionSize
          val xStartIndex = i * partitionSize
          for (j <- 0 until partitionSize) {
            for (k <- 0 until partitionSize) {
              if (j != k) {
                result(resultStartIndex + j * partitionSize + k) = (x(xStartIndex + j), x(xStartIndex + k))
              }
            }
          }
        }
        result.filter(_ != null).toSeq
      } else {
        val result = new Array[(String, String)](length * length)
        for (i <- 0 until length) {
          for (j <- 0 until length) {
            result(i * length + j) = (x(i), x(j))
          }
        }
        result.filter(_ != null).toSeq
      }
    })
      .join(followRDD) // a, b, followings
      .map(x => (x._2._1, x._2._2)) // b, followings
      .reduceByKey((a, b) => a + "\t" + b) // b recommended raw followings
      .join(followRDD) // b, b following, b recommended raw followings
      .map(x => {
      val followSet = x._2._2.split("\t").toSet // following set
      val candidates = x._2._1.split("\t").filter(y => y != x._1 && !followSet(y)).distinct // filtered recommends
      (x._1, candidates) // b, candidates
    }).filter(_._2.length != 0)
      .flatMap(x => x._2.map(y => (y, x._1))) // b, candidate

    val followerNumber = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= 1080 && x(1).toInt >= 1080)
      .map(x => (x(0), 1))
      .reduceByKey(_ + _) // follower number
      .filter(x => x._2 <= 1000 && x._2 >= 100)

    val recentlyActiveUser = sc.textFile(args(1))
      .filter(_.toInt >= 1080)
      .distinct(10)
      .map(x => (x, "_")) // recently active user

    recommendCandidates.join(followerNumber)
      .map(x => (x._1, (x._2._1, x._2._2))) // candidate, (b, c's follower number)
      .join(recentlyActiveUser) //recently active candidate, ((b, c's follower number), "_")
      .map(x => (x._2._1._1, x._1 + "," + x._2._1._2.toString)) // b, r a candidate, c's follower number
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => {
      val user = x._1
      val candidates = x._2.split("\t").map(_.split(",")).sortWith(_(1).toInt > _(1).toInt).map(_(0)).take(100).toSeq
      recommend(user, candidates)
    })
      .saveAsTextFile(args(2))
  }

}
