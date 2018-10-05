package cs6240.spark_twitter_follower

/**
 * @author Mahima Singh
 *
 * This class gets the number of followers for each twitter user, given edges.csv as input.
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd
object CountFollowersMainDSET {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ncs6240.spark_twitter_follower.CountFollowersMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count Followers")
    val sc = new SparkContext(conf)
    import org.apache.spark.sql.SparkSession
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val rdd = sc.textFile(args(0)).map(line => line.split(",")(1) )
    val dataset = sparkSession.createDataset(rdd)
    dataset.groupBy("Value").count().write.format("com.databricks.spark.csv").save(args(1))
    print("\n---------------------STARTING--------------------------\n") 
    dataset.explain()
    print("\n----------------------EXPLAIN STARTING----------------------------------------\n") 
    dataset.explain(true)
    print("\n-------------------------LOGICAL ENDING---------------------------------------------\n") 
  }
}