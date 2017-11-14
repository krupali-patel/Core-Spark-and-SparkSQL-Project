package com.krupali.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._


object MutualFriendCount {
  def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().
    setAppName("SparkExample") //.setMaster("local[*]").set("spark.driver.bindAddress","127.0.0.1")
val sc = new SparkContext(conf)
    
    
    val file = sc.textFile(args(0)) //"../soc-LiveJournal1Adj.txt"
    val parsedFile = file.map(x => x.split("\t")).filter(x => x.length == 2).map(x => (x(0), x(1).split(",")))
     
    val mapped_output = parsedFile.map(x=>
      {
         //var map1 = scala.collection.mutable.Map[String, List[String]]()
        val friend1 = x._1
        val friends = x._2.toList
        for(friend2<- friends) yield
        {
         if(friend1.toInt< friend2.toInt)
           (friend1+","+friend2 -> friends)
           else
          (friend2+","+friend1 -> friends)   
        }
        
      }
        
        )
     
     val flatten_output =   mapped_output.flatMap(identity).map(x=> (x._1,x._2)).distinct.reduceByKey((x,y)=> (x.intersect(y)))
     val friend_count = flatten_output.map(x=> (x._1, x._2.length))
     val formatted_output = friend_count.map(x=>s"${x._1}\t${x._2}")
     //flatten_output.coalesce(1).saveAsTextFile("Q1_output")
    formatted_output.coalesce(1).saveAsTextFile(args(1))

        }
}

