package edu.northeastern.ccs.cs6240.TwitterFollowerJoins
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd

/*
 * This class implements Reduce side join using RDD
 */
object RSJoinRDD {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ncs6240.spark_twitter_follower.CountFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count Followers")
    val sc = new SparkContext(conf)
    val original = sc.textFile(args(0))
                .map(line => {
                  val userId = line.split(",")
                  (userId(0), userId(1))})
                .filter(edge => edge._1.toInt < 10000 && edge._2.toInt <10000)
   
  // Here we swap as join in scala happens on keys but here we wish Left.values = Right.key              
   val swapped = original.map(item => item.swap)
    
   val RDD = original
                .join(swapped)
                .map{case(key,(value1,value2))=>(value1,value2)}
    
    val count = RDD.join(original).filter{ case (x,(y,z)) => y==z  }.count()  
                
    // .filter(word => word.split(",")(0).toInt < 100 && word.split(",")(1).toInt < 100) 
    
    println("NUMBER OF TRIANGLES " + count/3)
                
  
}
}