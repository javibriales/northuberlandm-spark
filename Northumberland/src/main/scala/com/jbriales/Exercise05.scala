package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * Exercise05 -   Count the occurrence of each word within the case text (CASETEXT field) across all planning application records
 * Assumptions: 
 * 		- All words will be transformed to lowercase to perform the count (assuming are the same words)
 * Doubts:
 * 		- Check data count before collect?? 
 */
object Exercise05 {
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 5")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    val res = df.select("CASETEXT") 
      .map { row => row.getString(0).toLowerCase() } // TRANSFORMATION: Maps row to "CASETEXT" data
      .flatMap { ct => ct.split("[ ,.:;?]") }        // TRANSFORMATION: mapping "CASETEXT" string into a sequence of Strings
      .filter { word => word.matches("[a-zA-Z]+") }  // TRANSFORMATION: Selects only string that represents real words, eg: 8kms won't be considered
      .map { word => (word, 1) }                     // TRANSFORMATION: Mapping to a pair where work is the key
      .reduceByKey(_ + _)                            // TRANSFORMATION: Reduce to count
      .map { case (word, count) => (count, word) }   // TRANSFORMATION: Maps (agent, count) to (count, agent) we will need to sort by key
      .sortByKey(false)                              // TRANSFORMATION: Short by key
                                           
    
      //OPTION: res.saveAsTextFile(outputPath) // Brings to Driver??? and writes to file
      
    // Creates PrintWriter     
    val writer = new PrintWriter(new File(outputPath))
    
    // Prints each word  Print at the driver
    res.collect().foreach(pair => writer.write("[%d]\t%s\n".format(pair._1, pair._2))) // ACTION: Brings to driver and prints
          
    // Closes Writer
    writer.close()
    
    sc.stop()
  }
}