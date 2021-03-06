package com.durgesh.software.sparkExercises

import com.durgesh.software.utils.CommonFunctions
import org.apache.log4j.{Level, Logger}

object exercise11 extends CommonFunctions with App {

  // logger to set for only error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = getSparkSession("exercise11")

  val sellersDF = spark.read.option("header","true").option("inferSchema","true")
    .csv("src/main/resources/sellers.csv")

  val productsDF = spark.read.option("header","true").option("inferSchema","true")
    .csv("src/main/resources/products.csv")

  val salesDF = spark.read.option("header","true").option("inferSchema","true")
    .csv("src/main/resources/sales.csv")





}
