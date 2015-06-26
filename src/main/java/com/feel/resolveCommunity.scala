package com.graph

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
 * Created by canoe on 6/26/15.
 */
case class recommend(user: String, candidate: Seq[String])

object resolveCommunity {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf())
    val followRDD = sc.textFile(args(0))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(1), x(0)))
    .reduceByKey((a, b) => a + "\t" + b)

    sc.textFile(args(1)).map(x => x.replaceAll("[({}):a-zA-Z]", "").split(",")).filter(_.length >= 2).map(x => (x(1), x(0)))
    .reduceByKey((a, b) => a + "\t" + b)
    .map(x => x._2.split("\t"))
    .filter(x => x.length >= 2 && x.length <= 1000)
    .flatMap(x => {
      val result = new Array[(String, String)](x.length * x.length)
      for (i <- 0 until x.length) {
        for (j <- 0 until x.length) {
          if (i != j) {
            result(i *x.length + j) = (x(i), x(j))
          }
        }
      }
      result.filter(_ != null).toSeq
    })
    .reduceByKey((a, b) => a + "\t" + b)
    .join(followRDD)
    .map(x => {
      val followSet = x._2._2.split("\t").toSet
      val candidate = x._2._1.split("\t").filter(x => !followSet(x)).take(100)
      recommend(x._1, candidate)
    })
    .saveAsTextFile(args(2))
  }

}
