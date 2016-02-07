package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * Exercise01 - Discover the schema of the input dataset and output it to a file
 */
object Exercise01 {
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 1")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    // Prints DataFrame schema to stdout
    //df.printSchema()
        
    // Prints to a file
    val writer = new PrintWriter(new File(outputPath))
      
    //writer.write(df.schema.prettyJson)  // Prints JSON schema
    writer.write(df.schema.treeString)
          
    writer.close()
    
    sc.stop()
  }
}