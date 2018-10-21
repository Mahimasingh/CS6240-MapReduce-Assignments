package ccs.neu.edu.hw3
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._ 



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
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val k = 100
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
    val graphDataFrame = sparkSession.createDataFrame(eRDD).toDF("v1","v2")
    var rankDataFrame = sparkSession.createDataFrame(prRDD).toDF("vertex","pr")
    
    for(i <- 1 to 10) {
    val joinedDataFrame = graphDataFrame.as("gdf")
                                      .join(rankDataFrame
                                       .as("rdf")
                                       ,$"gdf.v1" === $"rdf.vertex" ,"inner")
                                       .select("gdf.v2","rdf.pr")
                                       .toDF("vertex","pageRank")
    val noIcomingDataFrame = graphDataFrame.as("gdf")
                                      .join(rankDataFrame
                                       .as("rdf")
                                       ,$"gdf.v1" === $"rdf.vertex" ,"inner")
                                       .select("gdf.v1","rdf.pr")
                                       .toDF("vertex","pageRank")
                                       .filter($"vertex" % k === 1)
                                       .withColumn("pageRank",when(col("pageRank")!==0,0))
    val tempDataFrame = joinedDataFrame.union(noIcomingDataFrame)
    val groupedDataFrame = tempDataFrame.groupBy($"vertex").sum("pageRank").toDF("vertex","pageRank")
    
    val delta = groupedDataFrame.filter($"vertex" === 0)
                .select("pageRank")
                .first.getDouble(0) 
    
    val vertexNotZeroDataFrame = groupedDataFrame.filter($"vertex" !== 0)
                                  .select($"vertex",$"pageRank"+ delta / (k * k))
    val vertexZeroDataFrame = groupedDataFrame.filter($"vertex" === 0)
    rankDataFrame = vertexNotZeroDataFrame.union(vertexZeroDataFrame).toDF("vertex","pr")
    val sumPageRank = rankDataFrame.filter($"vertex"!== 0).groupBy().sum("pr").first().getDouble(0)
    joinedDataFrame.unpersist(true);
    noIcomingDataFrame.unpersist(true)
    tempDataFrame.unpersist(true)
    groupedDataFrame.unpersist(true)
    vertexNotZeroDataFrame.unpersist(true)
    vertexZeroDataFrame.unpersist(true)
    
    
    println("The sum of pageRank at" +i+"is "+ sumPageRank)
    }
    rankDataFrame.sort(desc("pr")).head(101).foreach(println)
    
    //.collect.foreach(println)
    
  }
  
}