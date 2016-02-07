package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * 
 * Exercise02 -  What is the total number of planning application records in the dataset
 * Assumptions: 
 * Doubts: 
 * 		- What's better, deal with DataFrame or with RDD??
 * 		- Print with rdd.saveAsTextFile does it distributed (a file per executor) or brings data to the driver and then prints
 */
object Exercise02 {
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 2")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    // Count using DataFrame
    val count = df.count()         // ACTION: Gets the number of elements in the DataFrame (records/rows)
    
    val writer = new PrintWriter(new File(outputPath))
      
    writer.write("%d".format(count))
      
    writer.close()
    
    sc.stop()
  }
}