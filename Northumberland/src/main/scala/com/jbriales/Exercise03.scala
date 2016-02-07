package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * Exercise03 -  Identify the set of case officers (CASEOFFICER field) 
 * Assumptions: 
 * 		- We don't need empty case officers
 * 		- We don't need a list with duplicates entries
 * Doubts: 
 * 		- What's better, deal with DataFrame or with RDD??
 * 		- Print with rdd.saveAsTextFile does it distributed or brings data to the driver
 */
object Exercise03 {
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 3")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    val res = df.select("CASEOFFICER")  
      .map(row => row.getString(0))        // TRANSFORMATION: Maps row to a String
      .filter { !_.isEmpty() }             // TRANSFORMATION: Gets no empties
      .distinct()                          // TRANSFORMATION: Eliminates duplicates  
      
    // println(result.toDebugString)        // DEBUG - Shows transformations lineage
    
    // I'm not sure about if, saveAsTextFile brings data to the drive or generates a file per cluster
    // Maybe it's needed to collect() data and write the contents
    //result.saveAsTextFile("exercise03.out")  // ACTION: ¿Brings data to driver? and write to a file. 
      
    val writer = new PrintWriter(new File(outputPath))
    
    // Collects and print
    res.collect().foreach { co => writer.write("%s\n".format(co)) }    // ACTION: Brings to driver and prints
      
    writer.close() 
    
    sc.stop()
  }
}