package cs6240.spark_twitter_follower

/**
 * @author Mahima Singh
 */

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.Row
/*
 * This class gets the number of followers for each twitter user, given edges.csv as input.
 */
object CountFollowersMainDSET {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ncs6240.spark_twitter_follower.CountFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    val wordCount = spark.sparkContext.textFile(args(0)).map(line => line.split(",")(1) ).toDF.groupBy("Value").count();
    wordCount.saveAsTextFile(args(1))
    
   print("\n---------------------STARTING--------------------------\n") 
   
   print(wordCount.toDebugString)
   
   
   print("\n----------------------ENDING----------------------------------------\n") 
  }
}