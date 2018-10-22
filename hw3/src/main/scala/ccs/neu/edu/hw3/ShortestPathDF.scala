package ccs.neu.edu.hw3
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._ 
object ShortestPathDF {
  
  class Vertex (src:Int,path:Int){
    var v: Int = src
    var d : Int = path
  }
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> k")
      System.exit(1)
    }
    val sparkSession = SparkSession.builder().appName("Shortest Path in Data Frame").getOrCreate()
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    var adjacencyListDF = sc.textFile(args(0))
                          .map(line => {
                           val userId = line.split(",")
                            (userId(0).toInt, userId(1).toInt)})
                            .groupByKey()
                            .map(x => (x._1, x._2.toList.sortBy(x => x)))
                            .toDF("v1","list")
    val k = args(1).toInt
    var sourceArray = new Array[Vertex](k)
    val sourceDF = adjacencyListDF
                        .sample(0.2,k+4)
                        .select($"v1")
                        .head(k)
    for(i <- 0 to k-1) {
        sourceArray.update(i
                     ,new Vertex(sourceDF(i).getAs(0),0))
     }
    
    for(i <- 0 to k-1) { 
     println("Starting for source value "+sourceArray(i).v)
     adjacencyListDF.collect().foreach(println)
     var activeVertices = Seq((sourceArray(i).v, 0)).toDF("vertex", "distance")
     adjacencyListDF.persist()
     var globalCountsDF = activeVertices
     
     while(!activeVertices.rdd.isEmpty()){
      var joinedDF = activeVertices.as("av").join(broadcast(adjacencyListDF).as("al"),$"av.vertex" === $"al.v1","inner")
                                             .select($"al.list",$"av.distance" + 1)
                                             .withColumn("al.list",explode($"al.list"))
                                             .toDF("col1","col2","col3")
                                             .select($"col3",$"col2")
                                             .toDF("vertex","distance")
      var filterVisitedChildren = joinedDF.as("jdf")
                                  .join(globalCountsDF.as("gdf"), $"jdf.vertex" === $"gdf.vertex","leftanti")
     
       globalCountsDF = globalCountsDF.union(filterVisitedChildren)
       activeVertices = filterVisitedChildren
      
    }
    var distance = globalCountsDF.sort(desc("distance")).select($"distance").first().getInt(0)
    println("Longest path is " + distance )
    sourceArray(i).d = distance;
    }
    
    for(i <- 0 to k-1) { 
      println("For vertex "+ sourceArray(i).v + "largest distance is " + sourceArray(i).d)
    } 
    
  
}
}