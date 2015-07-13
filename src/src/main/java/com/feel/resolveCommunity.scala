package com.feel

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by canoe on 6/27/15.
 */

object resolveCommunity {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf())
    val followRDD = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= 1080 && x(1).toInt >= 1080)
      .map(x => (x(1), x(0)))
      .reduceByKey((a, b) => a + "\t" + b)

    val recommendCandidates = sc.textFile(args(1)).map(x => x.replaceAll("[({}):a-zA-Z]", "").split(",")).filter(_
      .length >= 2).map(x => (x(1), x(0)))
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => x._2.split("\t"))
      .filter(x => x.length >= 2 && x.length <= 1000)
      .flatMap(x => {
      val result = new Array[(String, String)](x.length * x.length)
      for (i <- 0 until x.length) {
        for (j <- 0 until x.length) {
          if (i != j) {
            result(i * x.length + j) = (x(i), x(j))
          }
        }
      }
      result.filter(_ != null).toSeq
    })
      .join(followRDD)
      .map(x => (x._2._1, x._2._2))
      .reduceByKey((a, b) => a + "\t" + b)
      .join(followRDD)
      .map(x => {
      val followSet = x._2._2.split("\t").toSet
      val candidate = x._2._1.split("\t").filter(y => y != x._1 && !followSet(y)).distinct
      (x._1, candidate)
    }).filter(_._2.length != 0)
      .flatMap(x => x._2.map(y => (y, x._1)))

    val followerNumber = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= 1080 && x(1).toInt >= 1080)
      .map(x => (x(0), 1))
      .reduceByKey(_ + _)

    val recentlyActiveUser = sc.textFile(args(2))
    .filter(_.toInt >= 1080)
    .distinct
    .map(x => (x, "_"))

    recommendCandidates.join(followerNumber)
      .filter(_._2._2 >= 30)
      .map(x => (x._1, (x._2._1, x._2._2)))
      .join(recentlyActiveUser)
      .map(x => (x._2._1._1, x._1 + "," + x._2._1._2))
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => {
      val user = x._1
      val candidates = x._2.split("\t").map(_.split(",")).sortWith(_(1).toInt > _(1).toInt).map(_(0)).take(100)
      recommend(user, candidates)
    })
      .saveAsTextFile(args(3))
  }
}