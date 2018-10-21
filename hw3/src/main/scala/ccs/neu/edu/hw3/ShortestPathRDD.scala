package ccs.neu.edu.hw3
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import scala.util.Random
object ShortestPath {
  
  class Vertex (src:Int,path:Int){
    var v: Int = src
    var d : Int = path
  }
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Graph Shortest path")
    val sc = new SparkContext(conf);
    var adjacencyListRDD = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0).toInt, userId(1).toInt)})
                            .groupByKey()
                            .map(x => (x._1, x._2.toList.sortBy(x => x)))
                            
     
     var k = args(1).toInt
     var sourceArray = new Array[Vertex](k)
     val sourceRDD = adjacencyListRDD.sample(false,0.2).take(k+1)
     println("The value of k is" + k)
     println("The size of source RDD "+ sourceRDD.size)
     for(i <- 0 to k-1) {
        sourceArray.update(i
                     ,new Vertex(sourceRDD(i)._1,0))
     }
     adjacencyListRDD.persist()
     
     for(i <- 0 to k-1) { 
     println("The source vertex is "+sourceArray(i).v);
     
     var activeVertices = sc.parallelize( Seq((sourceArray(i).v, 0)) )
     var globalCounts = activeVertices
     while(!activeVertices.isEmpty()) {
       var rdd = adjacencyListRDD.join(activeVertices)
              .flatMap{v=> v._2._1.map(g => (g,v._2._2 +1))}
       var filterVisitedChildren = rdd.leftOuterJoin(globalCounts).filter(record => record._2._2 == None)
                                           .map(record => (record._1,record._2._1))
        globalCounts = globalCounts.union(filterVisitedChildren)
        activeVertices = filterVisitedChildren
     } 
    
    val element = globalCounts.map(item => item.swap) // interchanges position of entries in each tuple
                 .sortByKey(ascending = false) 
                 .first()._1
    println("The graph diameter is" + element)
    sourceArray(i).d = element;
   }
    for(i <- 0 to k-1) { 
      println("For vertex "+ sourceArray(i).v + "largest distance is " + sourceArray(i).d)
    }
    
  }
  
}