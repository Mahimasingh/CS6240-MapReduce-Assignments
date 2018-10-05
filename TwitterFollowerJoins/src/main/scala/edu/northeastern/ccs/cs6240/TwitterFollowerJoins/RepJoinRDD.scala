package edu.northeastern.ccs.cs6240.TwitterFollowerJoins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd

object RepJoinRDD {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ncs6240.spark_twitter_follower.CountFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count Followers")
    val sc = new SparkContext(conf);
    val originialRDD = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0), userId(1))})
                            .filter(edge => edge._1.toInt < 100 && edge._2.toInt <100)
                            
     val originalRDDMap = originialRDD.collectAsMap();
     val swapped = originialRDD.map(item => item.swap)
     val swappedMap = swapped.collectAsMap();
     val broadcasted = sc.broadcast(swappedMap)
     val results = originialRDD.flatMap {case(key, value) => broadcasted.value.get(key).map { otherValue =>
  (key, (value, otherValue))
   }
   }.map{case(key,(value1,value2))=>(value1,value2)}
   val count = results.join(originialRDD).filter{ case (x,(y,z)) => y==z  }.count() 
   println("NUMBER OF TRIANGLES" + count/3);
  
}
}