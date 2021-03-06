package com.durgesh.software.utils

import org.apache.spark.sql.SparkSession

import scala.io.Source

trait CommonFunctions {

  // Defining Spark Session

  def getSparkSession(appName: String) = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    spark
  }

  // Get configs into a Map object
  def getConfig(path: String): Map[String, String] = {
    var paramPairs = for {
      line <- Source.fromFile(path).getLines().filter(x => !x.trim.isEmpty && !x.startsWith("#"))
      split = line.split("=")
    } yield (split(0) -> split(1))

    paramPairs.toMap
  }
}
