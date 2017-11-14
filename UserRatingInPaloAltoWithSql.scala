package com.krupali.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

object UserRatingInPaloAltoWithSql {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setAppName("SparkExample")//. setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val businessSchema = StructType(Array(
      StructField("business_id", StringType, true),
      StructField("full_address", StringType, true),
      StructField("categories", StringType, true)))

    val reviewSchema = StructType(Array(
      StructField("review_id", StringType, true),
      StructField("user_id", StringType, true),
      StructField("business_id", StringType, true),
      StructField("stars", StringType, true)))

    val rawbusinessData = sc.textFile(args(0))
    val rowbusinessRDD = rawbusinessData.map(line => Row.fromSeq(line.split("::")))
    val businessDF = sqlContext.createDataFrame(rowbusinessRDD, businessSchema)
    businessDF.createOrReplaceTempView("business_view")

    val rawReviewData = sc.textFile(args(1))
    val rowReviewRDD = rawReviewData.map(line => Row.fromSeq(line.split("::")))
    val reviewDF = sqlContext.createDataFrame(rowReviewRDD, reviewSchema)
    reviewDF.createOrReplaceTempView("review_view")

    val id_paloALto = sqlContext.sql("""SELECT business_id FROM business_view where UPPER(full_address) LIKE '%PALO ALTO%'""")
    id_paloALto.createOrReplaceTempView("id_paloALto_view")
    val joined_DF = sqlContext.sql("""SELECT r.user_id , AVG(r.stars) as average FROM id_paloALto_view l INNER JOIN review_view r ON l.business_id = r.business_id GROUP BY r.user_id""")
    
    joined_DF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(args(2))
    //println(joined_DF.count())

  }
}