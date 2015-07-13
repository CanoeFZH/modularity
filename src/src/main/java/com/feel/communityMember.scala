package com.feel

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 7/8/15.
 */
object communityMember {
 def main(args: Array[String]) = {
  val sc = new SparkContext(new SparkConf())

  sc.textFile(args(0)).map(x => x.replaceAll("[({}):a-zA-Z]", "").split(",")).filter(_.length >= 2).map(x => (x(1), x
    (0)))
    .reduceByKey((a, b) => a + "\t" + b)
  .saveAsTextFile(args(1))
 }

}
