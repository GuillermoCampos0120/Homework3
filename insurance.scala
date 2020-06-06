package com.spark.guillermo
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.log4j._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.functions._








object insurance {
  
  def main(args : Array[String]): Unit = {
    val filename = "C:/spark/insurance.csv"
    for(line <- Source.fromFile(filename).getLines){
      println(line)
      println(SizeEstimator.estimate(filename))
     val fp = filtertable
     .select("sex")
     .groupBy("sex")
     .agg(first("sex"))
     .select("sex")
      
      }
  }