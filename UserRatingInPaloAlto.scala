package com.krupali.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object UserRatingInPaloAlto {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setAppName("SparkExample")//.setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    val businessFile = sc.textFile(args(0))
    val businessLine = businessFile.map(parseBusinessFile)
    val business_PaloAlto = businessLine.filter(x => x._2.toUpperCase.contains("PALO ALTO"))

    val reviewFile = sc.textFile(args(1))
    val reviewLine = reviewFile.map(parseReviewFile)

    val user_ratings = business_PaloAlto.join(reviewLine).map(x => (x._2._2._1, x._2._2._2))
    val sum_user_ratings = user_ratings.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avg_user_ratings = sum_user_ratings.mapValues(x => x._1 / x._2)

    val formatted_output = avg_user_ratings.map(line =>s"${line._1}\t${line._2}")
   

    formatted_output.foreach(println)
    formatted_output.coalesce(1).saveAsTextFile(args(2))
   
     //avg_user_ratings.map(a => a._1 + "," + a._2.toString()).saveAsTextFile("Q4.csv")

    //avg_user_ratings.foreach(println)

  }
  def parseBusinessFile(line: String) =
    {
      val fields = line.split("::")
      val business_id = fields(0)
      val full_address = fields(1)
      (business_id, full_address)
    }

  def parseReviewFile(line: String) =
    {
      val fields = line.split("::")
      val user_id = fields(1)
      val business_id = fields(2)
      val stars = fields(3).toDouble
      (business_id, (user_id, stars))
    }
}