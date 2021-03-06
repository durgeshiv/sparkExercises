package com.durgesh.software.sparkExercises

import org.apache.log4j._
import com.durgesh.software.utils.constants
import com.durgesh.software.utils.CommonFunctions
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, expr, sum}

object basicTransformations extends CommonFunctions {

  def main(args: Array[String]): Unit = {

    //setting log level to only display errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Param path to be taken as program argument
    val configPath = args(0)
    // Get config params for application
    val getConf = getConfig(configPath)

    // get spark session from utility package
    val spark = getSparkSession("Spark Basic Transformation")

    // Reading file from resources
    val inputDF = spark.read.format(getConf(constants.INPUT_FILE_FORMAT))
      .option("header", getConf(constants.HEADER_ENABLED))
      .option("inferSchema","true")
      .load(getConf(constants.INPUT_FILE_PATH))

    inputDF.printSchema()
    inputDF.show(1, false)

    // solving for problem 1
    // val countDF = inputDF.select("FlightNum").distinct().count()
    val delaycount = inputDF.withColumn("arrDelayedCount", expr("count(CASE WHEN IsArrDelayed = 'YES' then 1 END)"))
    .withColumn("depDelayedCount", expr("count(CASE WHEN IsDepDelayed = 'YES' then 1 END)"))
    .withColumn("FlightCount", expr("count(FlightNum")).select("arrDelayedCount", "depDelayedCount", "FlightCount")


    delaycount.show()


  }

}