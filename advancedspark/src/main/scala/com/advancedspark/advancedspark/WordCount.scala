package com.advancedspark.advancedspark

import org.apache.spark._

//Testing the Maven Build  with word Count

object WordCount {
  
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
    
    val inputFile = sc.textFile("food.txt")
    
    inputFile.flatMap { line => 
      line.split(" ")  
    }
    .map { word =>
        (word, 1)
      }
    .reduceByKey( _ + _ )
    .saveAsTextFile("foodCount.txt")
    sc.stop()
  }
}