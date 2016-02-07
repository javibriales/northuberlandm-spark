package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * Exercise04 -  Who are the top N agents (AGENT field) submitting the most number of applications
 * Assumptions: 
 * 		- We don't need empty agents
 * Doubts: 
 * 		- What's better, deal with DataFrame or with RDD??
 * 		- Print with rdd.saveAsTextFile does it distributed or brings data to the driver
 */
object Exercise04 {
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 4")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    val topNValue = args(2)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    val res = df.select("AGENT")
      .map { row => row.getString(0) }               // TRANSFORMATION: Maps row to agent data
      .filter { !_.isEmpty() }                       // TRANSFORMATION: Gets no empties
      .map { agent => (agent, 1) }                   // TRANSFORMATION: Maps to (agent, 1)
      .reduceByKey(_ + _)                            // TRANSFORMATION: Aggregates values by key
      .map { case (agent, count) => (count, agent) } // TRANSFORMATION: Maps (agent, count) to (count, agent)
      .sortByKey(false)                              // TRANSFORMATION: Sorts by key (desc)

    val topN = res.take(topNValue.toInt)         // ACTION: Brings RDD to the Driver an takes top N
        
    // Creates PrintWriter     
    val writer = new PrintWriter(new File(outputPath))
    
    // Prints each agent     
    topN.foreach(pair => writer.write("[%d]\t%s\n".format(pair._1, pair._2)))
    
    // Closes Writer
    writer.close()
    
    sc.stop()
  }
}