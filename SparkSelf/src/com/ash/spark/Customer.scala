package com.ash.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions



object Customer {
  
  def getCustomer_amount(line:String)={
   
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)  
    
  } 
  
  
  
  def main(args: Array[String]): Unit = {
    
  
  //logs
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //creating sparksession
  val spark=SparkSession.builder()
     .master("local[*]")
     .appName("CustomerOrder")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()
  //generating sparkContext through sparksession
  val  sc =spark.sparkContext
  // inserting data from local drive to rdd
  val lines = sc.textFile("../customer-orders.csv")
 
  val cust_spent = lines.map(getCustomer_amount)
 
  val total_spent = cust_spent.reduceByKey((x,y) => x+y)
  
  val fliped = total_spent.map(x => (x._2, x._1))
  //calling action on rdd
  val results = fliped.sortByKey(true).collect()
  
  results.foreach(println)
  
  spark.stop()  
  
  }
  
}