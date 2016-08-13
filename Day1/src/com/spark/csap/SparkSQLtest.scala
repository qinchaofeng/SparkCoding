package com.spark.csap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.avro.ipc.specific.Person
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object lession1 {
  case class Person(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql test").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._
    
    //转为RDD
    val people = sc.textFile("D:\\Apache\\spark-1.6.2-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt").map(_.split(",")).map(p => Person(p(0),p(1).trim.toInt)).toDF()
    
    people.registerTempTable("people")
    
    val man = sqlContext.sql("SELECT name, age FROM people ")
    man.map { t => "name: " + t(0) + " " + "age :" + t(1) }.collect().foreach(println)
    
  }
  
}