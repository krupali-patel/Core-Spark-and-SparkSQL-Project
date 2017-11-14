package com.krupali.spark


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
object TopTenBusiness {
  
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
   // val sc = new SparkContext("local[*]", "MinTemperatures")
  val conf = new SparkConf().
    setAppName("SparkExample")//.setMaster("local[*]").set("spark.driver.bindAddress","127.0.0.1")
val sc = new SparkContext(conf)

    val reviewlines = sc.textFile(args(0))
    val businessline = sc.textFile(args(1))
    
   
    
    // Convert to (business_id, ratings tuples
    val parsedreviewLines = reviewlines.map(parseReviewFile)
    
    val business_ratingRDD = parsedreviewLines.map(x=> (x._1,1)).reduceByKey((x,y) => (x+y))
   
    val parsedbusinessLines = businessline.map(parseBusinessFile)
    
    val business_details = business_ratingRDD.join(parsedbusinessLines).distinct().collect()
    
    val res= business_details.sortWith(_._2._1 > _._2._1).take(10)
    
   val formatted_output = res.map(x =>s"${x._1}\t${x._2._1}\t${x._2._2._1}\t${x._2._2._2}")
   
val formatted_outputRDD = sc.parallelize(formatted_output)
formatted_outputRDD.foreach(println)
formatted_outputRDD.coalesce(1).saveAsTextFile(args(2))
   
  
  }
  def parseReviewFile(line:String) =
  {
    val fields = line.split("::")
  
    val business_id = fields(2)
    val ratings = fields(3).toDouble
    (business_id,ratings)

  }
   def parseBusinessFile(line:String) =
  {
    val fields = line.split("::")
    val business_id = fields(0)
    val full_address = fields(1)
    val categories = fields(2)
    (business_id,(full_address,categories))
  }
}