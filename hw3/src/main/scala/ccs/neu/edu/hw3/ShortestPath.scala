package ccs.neu.edu.hw3
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import scala.util.Random
object ShortestPath {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Graph Shortest path")
    val sc = new SparkContext(conf);
    var adjacencyListRDD = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0).toInt, userId(1).toInt)})
                            .groupByKey()
                            .map(x => (x._1, x._2.toList.sortBy(x => x)))
                            
     
     // TO-DO : sample and get k sources
     //val source = adjacencyListRDD.take(1)(0)._1
     val source = adjacencyListRDD.sample(false,0.2).take(1)(0)._1
     println("The source vertex is "+source);
     adjacencyListRDD.persist()
     var activeVertices = sc.parallelize( Seq((source, 0)) )
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
    
  }
  
}