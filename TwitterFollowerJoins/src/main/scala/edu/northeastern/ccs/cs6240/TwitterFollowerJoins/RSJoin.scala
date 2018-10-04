package edu.northeastern.ccs.cs6240.TwitterFollowerJoins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

/**
 * @author ${user.name}
 */
object RSJoinMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RSJoin")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val counts = textFile.filter(line => line.split(",")(0).toInt < 100 && line.split(",")(1).toInt <100)
                 
    counts.saveAsTextFile(args(1))


}
}
