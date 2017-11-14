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
import scala.collection.mutable.WrappedArray
object MutualFriendCountWithSQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setAppName("SparkExample")//.setMaster("local[*]"). set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val file = sc.textFile(args(0))
    val parsedFile = file.map(x => x.split("\t")).filter(x => x.length == 2).map(x => (x(0), x(1).split(",")))
    val mapped_output = parsedFile.map(x =>
      {
        //var map1 = scala.collection.mutable.Map[String, List[String]]()
        val friend1 = x._1
        val friends = x._2.toList
        for (friend2 <- friends) yield {
          if (friend1.toInt < friend2.toInt)
            (friend1 + "," + friend2, friends)
          else
            (friend2 + "," + friend1, friends)
        }

      }).flatMap(identity).map(x => (x._1, x._2)).map(x => (x._1.split(","), x._2)).map(x => (x._1(0), x._1(1), x._2))
    val spark = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._
    val table1 = mapped_output.toDF().distinct()
    val table2 = mapped_output.toDF().distinct()
    var table3 = table1.join(table2, (table1("_1") === table2("_1") && table1("_2") === table2("_2")))
    table3 = table3.filter(table1("_3") !== table2("_3"))

    val table4 = table3.select(table1("_1").alias("user1"), table1("_2").alias("user2"), table1("_3").alias("f_1"), table2("_3").alias("f_2"))
    //import org.apache.spark.sql.functions.udf
    // spark.udf.register("find_count", find_count _)
    val find_count = udf {
      (Set1: WrappedArray[String], Set2: WrappedArray[String]) =>
        (Set1.toList.intersect(Set2.toList)).length
    }
    val table5 = table4.withColumn("count", find_count($"f_1", $"f_2")).select("user1", "user2", "count").distinct()
    table5.show()
    table5.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(args(1)) // table5.orderBy(desc("count")).limit(10).show();
  }

  //  def find_count(list1: WrappedArray[String], list2: WrappedArray[String]): Int =
  //    {
  //      (list1.toList.intersect(list2.toList)).length
  //    }
}