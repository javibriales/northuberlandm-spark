package com.jbriales

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.io.PrintWriter
import java.io.File

/**
 * @author jbriales
 */
object App {
 
  
  def main(args : Array[String]) {
    
    // Creates SparkConf
    val sparkConf = new SparkConf().setAppName("Northumberland Analysis")//.setMaster("local")

    // Creates SparkContext
    val sc = new SparkContext(sparkConf)
    
    // SQLContext
    val sqlContext = new SQLContext(sc)

    // Path to data source
    val pathToData = args(0)
    val topNValue = args(1)
    
    // Read JSON as DataFrame
    val df = sqlContext.read.json(pathToData); // Distributed collection of data organized into named columns    
    
    
    // RUNNING EXERCISES
    runExercise1(df)
    runExercise2(df)
    runExercise3(df)
    runExercise4(df, topNValue.toInt)
    runExercise5(df)
    runExercise6(df)
    
    sc.stop()
  }
  
  
  /*
   * Exercise 1: Print JSON Schema 
   */
  def runExercise1(df: DataFrame) {

    // Prints DataFrame schema to stdout
    //df.printSchema()
        
    // Prints to a file
    val writer = new PrintWriter(new File("exercise01.out"))
      
    //writer.write(df.schema.prettyJson)  // Prints JSON schema
    writer.write(df.schema.treeString)
          
    writer.close()
   
  }
  
  /*
   * Exercise 2: What is the total number of planning application records in the dataset
   * Assumptions: 
   * Doubts: 
   * 		- What's better, deal with DataFrame or with RDD??
   * 		- Print with rdd.saveAsTextFile does it distributed or brings data to the driver
   */
  def runExercise2(df: DataFrame) {

    // Count using DataFrame
    val count = df.count()         // ACTION: Gets the number of elements in the DataFrame (records/rows)
    
    val writer = new PrintWriter(new File("exercise02.out"))
      
    writer.write("%d".format(count))
      
    writer.close()

  }
  
  /*
   * Exercise 3: Identify the set of case officers (CASEOFFICER field)
   * Assumptions: 
   * 		- We don't need empty case officers
   * 		- We don't need a list we duplicates entries
   * Doubts: 
   * 		- What's better, deal with DataFrame or with RDD??
   * 		- Print with rdd.saveAsTextFile does it distributed or brings data to the driver
   */
  def runExercise3(df: DataFrame) {
    
    val res = df.select("CASEOFFICER")  
      .map(row => row.getString(0))        // TRANSFORMATION: Maps row to a String
      .filter { !_.isEmpty() }             // TRANSFORMATION: Gets not empties
      .distinct()                          // TRANSFORMATION: Eliminates duplicates   
      .collect()                           // ACTION: Brings to driver
      
    // println(result.toDebugString)        // DEBUG - Shows transformations lineage
    
    //result.saveAsTextFile("exercise03.out")  // ACTION: ¿Brings data to driver? and write to a file. 
      
    val writer = new PrintWriter(new File("exercise03.out"))
      
    res.foreach { co => writer.write("%s\n".format(co)) }
      
    writer.close()        
    
    
    // I'm not sure if saveAsTextFile brings data to the drive or generates a file per cluster
    // Maybe it's needed to collect() data and write the contents
    
  }
  
  /*
   * Exercise 4:  Who are the top N agents (AGENT field) submitting the most number of applications
   * Assumptions: 
   * 		- We don't need empty agents
   * Doubts: 
   * 		- What's better, deal with DataFrame or with RDD??
   * 		- Print with rdd.saveAsTextFile does it distributed or brings data to the driver
   */
  def runExercise4(df: DataFrame, n:Int) {

    val res = df.select("AGENT")
      .map { row => row.getString(0) }     // TRANSFORMATION: Maps row to agent data
      .filter { !_.isEmpty() }             // TRANSFORMATION: Gets not empties
      .map { agent => (agent, 1) }         // TRANSFORMATION: Maps to (agent, 1)
      .reduceByKey(_ + _)                  // TRANSFORMATION: Aggregates values by key
      .map { case (agent, count) => (count, agent) } // TRANSFORMATION: Maps (agent, count) to (count, agent)
      .sortByKey(false)                    // TRANSFORMATION: Sorts by key (desc) - 

    val topN = res.take(n)                 // ACTION: Brings RDD to the Driver an takes top N
        
    // Creates PrintWriter     
    val writer = new PrintWriter(new File("exercise04.out"))
    
    // Prints each agent     
    topN.foreach(pair => writer.write("[%d]\t%s\n".format(pair._1, pair._2)))
    
    // Closes Writer
    writer.close()
    
  }
  
  /*
   * Exercise 5: Count the occurrence of each word within the case text (CASETEXT field) across all planning application records.
   * Assumptions: 
   * 		- All words will be transformed to lowercase to perform the count (assuming are the same words)
   * Doubts:
   * 		- Check data count before collect?? 
   */
  def runExercise5(df: DataFrame) {

    val res = df.select("CASETEXT") 
      .map { row => row.getString(0).toLowerCase() } // TRANSFORMATION: Maps row to "CASETEXT" data
      .flatMap { ct => ct.split("[ ,.:;?]") }        // TRANSFORMATION: mapping "CASETEXT" string into a sequence of Strings
      .filter { word => word.matches("[a-zA-Z]+") }  // TRANSFORMATION: Selects only string that represents real words, eg: 8kms won't be considered
      .map { word => (word, 1) }                     // TRANSFORMATION: Mapping to a pair where work is the key
      .reduceByKey(_ + _)                            // TRANSFORMATION: Reduce to count
      .map { case (word, count) => (count, word) }   // TRANSFORMATION: Maps (agent, count) to (count, agent) we will need to sort by key
      .sortByKey(false)                              // TRANSFORMATION: Short by key
      .collect()                                     // ACTION: Brings to driver
    
      //OPTION: res.saveAsTextFile("exercise05.out") // Brings to Driver and writes to file before invoking collect()
      
      // Creates PrintWriter     
      val writer = new PrintWriter(new File("exercise05.out"))
      
      // Prints each word  Print at the driver
      res.foreach(pair => writer.write("[%d]\t%s\n".format(pair._1, pair._2)))
            
      // Closes Writer
      writer.close()

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
  
  
  /*
   * Exercise 6: Measure the average public consultation duration in days
   * Assumptions: 
   * 		- All words will be transformed to lowercase to perform the count (assuming are the same words)
   * Doubts:
   * 		- Check data count before collect?? 
   */
  def runExercise6(df: DataFrame) {    

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
    val writer = new PrintWriter(new File("exercise06.out"))
    
    // Prints averages    
    val averageInt: Int = averageDays._1 / averageDays._2
    val averageDouble: Double = averageDays._1.toDouble / averageDays._2.toDouble
    writer.write("%d [%f]".format(averageInt, averageDouble))
          
    // Closes Writer
    writer.close()   
  }

}
