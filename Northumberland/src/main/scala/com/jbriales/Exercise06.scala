package com.jbriales

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 * Exercise06 -  Measure the average public consultation duration in days 
 * (i.e. the difference between PUBLICCONSULTATIONENDDATE and PUBLICCONSULTATIONSTARTDATE fields)
 * Assumptions: 
 * 		- All words will be transformed to lowercase to perform the count (assuming are the same words)
 * Doubts:
 * 		- Check data count before collect?? 
 */
object Exercise06 {
  
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis - Exercise 6")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val inputPath = args(0)
    val outputPath = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(inputPath); // Distributed collection of data organized into named columns    
        
    // Registering UDF function to uses as part of HiveSQL
    df.sqlContext.udf.register("transformDate", transformDate(_: String))

    // Register dataframe as table to enable queries
    df.registerTempTable("publiccons");

    // OPTION 1: Get average using SparkSQL
    //val avergeDaysSQL = df.sqlContext.sql("select avg(datediff(transformDate(PUBLICCONSULTATIONENDDATE), transformDate(PUBLICCONSULTATIONSTARTDATE))) FROM publiccons where (PUBLICCONSULTATIONENDDATE is not null and PUBLICCONSULTATIONENDDATE <> '') and (PUBLICCONSULTATIONSTARTDATE is not null and PUBLICCONSULTATIONSTARTDATE <> '')")
    //avergeDaysSQL.show() // Shows the result
   
    // OPTION 2: Get average with transformations
    val averageDays = df.sqlContext.sql("select datediff(transformDate(PUBLICCONSULTATIONENDDATE), transformDate(PUBLICCONSULTATIONSTARTDATE)) FROM publiccons where (PUBLICCONSULTATIONENDDATE is not null and PUBLICCONSULTATIONENDDATE <> '') and (PUBLICCONSULTATIONSTARTDATE is not null and PUBLICCONSULTATIONSTARTDATE <> '')")
      .map { row => (row.getInt(0), 1) }                   // TRANSFORMATION: Maps row to datediff (days, 1)    
      .reduceByKey(_ + _)                                  // TRANSFORMATION: Reduce to count (record with same number of days)
      .map { case (days, count) => (count * days, count) } // TRANSFORMATION: Map to (total days, num of records)
      .reduce { (a, b) => (a._1 + b._1, a._2 + b._2) }     // ACTION: aggregation into (total days, num of records)

       
    // Creates PrintWriter     
    val writer = new PrintWriter(new File(outputPath))
    
    // Prints averages    
    val averageInt: Int = averageDays._1 / averageDays._2
    val averageDouble: Double = averageDays._1.toDouble / averageDays._2.toDouble
    writer.write("%d [%f]".format(averageInt, averageDouble))
          
    // Closes Writer
    writer.close()   
    
    sc.stop()
  }

  /*
   * User-Defined Function: "transfromDate" to transform dates patters from "dd/MM/yyyy" to "yyyy-MM-dd" 
   * Assumptions: 
   * 		- All input string will have the right pattern. Otherwise, control it using regexps
   * 
   */
  def transformDate(date: String) = {
    val tmp = date.split("/")

    "%s-%s-%s".format(tmp(2), tmp(1), tmp(0))

  }
}