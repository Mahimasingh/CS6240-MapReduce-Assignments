package edu.northeastern.ccs.cs6240.TwitterFollowerJoins
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object RepJoinMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val sparkSession = SparkSession.builder().appName("RS Join").getOrCreate()
    import sparkSession.implicits._
    var x = args(2)
    val dSET = sparkSession.read.csv(args(0)).toDF("from", "to").filter(s"from <= $x and to <= $x")
    val interDset = dSET.as("Left").join(broadcast(dSET).as("Right"),$"Left.to" === $"Right.from", "inner")
                        .select("Left.from", "Right.to")
    val count = interDset.as("Left")
                          .join(broadcast(dSET).as("Right"),$"Left.from" === $"Right.to" && $"Left.to" === $"Right.from","inner")
                          .count()
    println("THE NUMBER OF TRIANGLES "+ count/3)
  
  }}