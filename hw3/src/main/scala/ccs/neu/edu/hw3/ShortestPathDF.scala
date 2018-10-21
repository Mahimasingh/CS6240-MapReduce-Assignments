package ccs.neu.edu.hw3
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
object ShortestPathDF {
  
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
    val source = adjacencyListDF.sample(0.2,k+1).select($"v1").first().getInt(0)
    println("The source is "+source)
    adjacencyListDF.collect().foreach(println)
    var activeVertices = Seq((source, 0)).toDF("vertex", "distance")
    adjacencyListDF.persist()
    var globalCountsDF = activeVertices
    while(!activeVertices.rdd.isEmpty()){
      var joinedDF = activeVertices.as("av").join(adjacencyListDF.as("al"),$"av.vertex" === $"al.v1","inner")
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
    
    globalCountsDF.collect().foreach(println)
    
  
}
}