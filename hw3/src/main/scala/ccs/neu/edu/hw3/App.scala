package ccs.neu.edu.hw3

/**
 * @author ${user.name}
 */
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
// import classes required for using GraphX
import org.apache.spark.graphx._
object App {
  
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
    for(i <- 1 to 3) {
      val joinRDD = eRDD.join(prRDD).map(joined => (joined._1,joined._2._1,joined._2._2))
      val tempRDD = joinRDD.map(triple => (triple._2,triple._3))
      val temp2RDD = tempRDD.reduceByKey(_ + _)
      val delta = temp2RDD.lookup(0)(0)
      prRDD = temp2RDD.map(vertex => if(vertex._1 !=0) (vertex._1, (vertex._2 + delta / (k*k).toDouble)) else (vertex._1,vertex._2))
      val sum = prRDD.map(_._2).sum()
      println("The sum of all Page Ranks at" +i+ "iteration"+ sum)
      prRDD.foreach(println)
     
    }
    
    prRDD.foreach(println)
    
    
    
  }
  

}
