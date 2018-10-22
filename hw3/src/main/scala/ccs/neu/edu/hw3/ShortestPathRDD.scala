package ccs.neu.edu.hw3
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.HashPartitioner
object ShortestPathRDD {
  
  class Vertex (src:Int,path:Int){
    var v: Int = src
    var d : Int = path
  }
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Graph Shortest path")
    val sc = new SparkContext(conf);
    // Creating an adjacency list 
    var adjacencyListRDD = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0).toInt, userId(1).toInt)})
                            .groupByKey()
                            .map(x => (x._1, x._2.toList.sortBy(x => x)))
                            
     
     // Giving the value of k
     var k = args(1).toInt
     var sourceArray = new Array[Vertex](k)
     val sourceRDD = adjacencyListRDD.sample(false,0.2).take(k+1)
     println("The value of k is" + k)
     // Assigning values for initial sources and their longest shortest distance 
     for(i <- 0 to k-1) {
        sourceArray.update(i
                     ,new Vertex(sourceRDD(i)._1,0))
     } 
     adjacencyListRDD.persist()
     
     // For each source vertex
     for(i <- 0 to k-1) { 
      println("The source vertex is "+sourceArray(i).v);
     
      var activeVertices = sc.parallelize( Seq((sourceArray(i).v, 0)) )
      var globalCounts = activeVertices
     // BFS begins
      while(!activeVertices.isEmpty()) {
       var rdd = //adjacencyListRDD.join(activeVertices)
                 joinRDDwithPartitioner(adjacencyListRDD,activeVertices)
                .flatMap{v=> v._2._1.map(g => (g,v._2._2 +1))}
       println("Joined successfully with adjacency list to obtain next neighbors");
       var filterVisitedChildren = rdd.leftOuterJoin(globalCounts).filter(record => record._2._2 == None)
                                           .map(record => (record._1,record._2._1))
       println("Removing already processed children");
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
  
  def joinRDDwithPartitioner (graphRDD: RDD[(Int, List[Int])],
   activeVertexRDD: RDD[(Int, Int)]) : RDD[(Int, (List[Int], Int))]= {
    
      // If activeVertexRDD has a known partitioner we should use that,
    // otherwise it has a default hash parttioner, which we can reconstruct by
    // getting the number of partitions.
    
      val activeVertexPartitioner = activeVertexRDD.partitioner match {
      case (Some(p)) => p
      case (None) => new HashPartitioner(activeVertexRDD.partitions.length)
    }
    var graphRDDpartitioned = graphRDD.partitionBy(activeVertexPartitioner)
    return graphRDDpartitioned.join(activeVertexRDD)
  }
    
  
}