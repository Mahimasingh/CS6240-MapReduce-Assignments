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
    val adjacencyListRDD = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0).toInt, userId(1).toInt)})
                            .groupByKey()
                            .map(x => (x._1, x._2.toList.sortBy(x => x)))
                            
     
     // TO-DO : sample and get k sources
     val source = adjacencyListRDD.take(1)(0)._1
     /*val newAdjacencyListRDD = adjacencyListRDD
                               .map(element => if(element._1 == source) (element._1,true,0,element._2) else (element._1,false,Int.MinValue,element._2))
     
     def bfs(source : Int, distanceOfSource : Int, adjacencyList : RDD[(Int,Boolean,Int,List[Int])], maxDistance : Int) {
       
    } */
     println("The source is "+source)
     adjacencyListRDD.persist()
     // seems active vertices keep track of 
     var activeVertices = sc.parallelize( Seq((source, 0)) )
     var rdd = adjacencyListRDD.join(activeVertices)
              //.map{case(key,(value1,value2))=>(value1,value2)}
              .flatMap{v=> v._2._1.map(g=>(g,v._2._2 +1))}
    
     rdd.foreach(println)
    
  }
  
}