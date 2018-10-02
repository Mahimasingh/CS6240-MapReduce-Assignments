package cs6240.spark_twitter_follower

/**
 * @author Mahima Singh
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd
/*
 * This class gets the number of followers for each twitter user, given edges.csv as input.
 */
object CountFollowersMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ncs6240.spark_twitter_follower.CountFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count Followers")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    val wordCount = sc.textFile(args(0)).map(line => line.split(",")(1) ).map((_, 1)).reduceByKey(_ + _)
   print("\n---------------------STARTING--------------------------\n") 
   
   print(wordCount.toDebugString)
   wordCount.saveAsTextFile(args(1))
   
   print("\n----------------------ENDING----------------------------------------\n") 
  }
}