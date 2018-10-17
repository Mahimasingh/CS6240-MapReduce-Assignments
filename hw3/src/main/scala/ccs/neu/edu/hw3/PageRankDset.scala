package ccs.neu.edu.hw3
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object PageRankDset {
  class Edges (src:Int,dest:Int){
    var x: Int = src
    var y : Int = dest
  }
  
  class PageRank(node:Int,pr:Double){
    var v : Int = node
    var pageRank : Double = pr
  }
  def main(args : Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val sparkSession = SparkSession.builder().appName("page Rank").getOrCreate()
    val conf = new SparkConf().setAppName("Creating graph")
    val sc = new SparkContext(conf)
    val k = args(0).toInt 
    var edgesArray = new Array[Edges](k*k)
    val dummy = new PageRank(0,0)
    var pageRanks = Array(dummy)
    for (x <- 1 to k*k) {
      if(x%k == 0){
        edgesArray.update(x-1, new Edges(x,0))
      }
      else {
        edgesArray.update(x-1, new Edges(x,x+1))
      }
      pageRanks:+=new PageRank(x,1.0/(k*k).toDouble)
    }
    var eRDD = sc.parallelize(edgesArray.map(edges => (edges.x, edges.y)), 2)
    var prRDD = sc.parallelize(pageRanks.map(pageRank => (pageRank.v,pageRank.pageRank)), 2)
    val graphDataFrame = sparkSession.createDataFrame(eRDD)
    var rankDataFrame = sparkSession.createDataFrame(prRDD)
    //for(i <- 1 to 10) {
      val joinedDataSet = graphDataFrame.join(rankDataFrame)
      joinedDataSet.collect.foreach(println)
    //}
  }
  
}