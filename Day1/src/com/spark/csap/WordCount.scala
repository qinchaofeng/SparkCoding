package com.spark.csap

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object WordCount {
  def  main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("MY frist Spark App")
    conf.setMaster("local")
    
    var sc = new SparkContext(conf)
    
    val lines = sc.textFile("D:\\Apache\\spark-1.6.2-bin-hadoop2.6\\README.md", 1)
    
    val words = lines.flatMap { line => line.split(" ") }
    
    val pairs = words.map { word => (word,1) }
    
    val WordCount = pairs.reduceByKey(_+_)
    WordCount.cache()
    WordCount.saveAsTextFile("E:\\output.dat")
    
   // WordCount.foreach(wordNumberPair=>println(wordNumberPair._1 + ":" + wordNumberPair._2))
    
   // sc.stop()
    
  }
}