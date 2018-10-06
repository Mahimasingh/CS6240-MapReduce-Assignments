package edu.northeastern.ccs.cs6240.TwitterFollowerJoins
import org.apache.log4j.LogManager
import java.io.PrintWriter
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
/*
 * This class implements Replicated join using Data Set
 */
object RepJoinMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val sparkSession = SparkSession.builder().appName("RS Join").getOrCreate()
    import sparkSession.implicits._
    var x = 500
    val dSET = sparkSession.read.csv(args(0)).toDF("from", "to").filter(s"from <= $x and to <= $x")
    val interDset = dSET.as("Left").join(broadcast(dSET).as("Right"),$"Left.to" === $"Right.from", "inner")
                        .select("Left.from", "Right.to")
    val count = interDset.as("Left")
                          .join(broadcast(dSET).as("Right"),$"Left.from" === $"Right.to" && $"Left.to" === $"Right.from","inner")
                          .count()
    logger.info("THE NUMBER OF TRIANGLES "+ count/3)
    println("THE NUMBER OF TRIANGLES "+ count/3)
   
  }}