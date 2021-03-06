/**
 *
 * Given a scenario, we need to identify people whose salary is less than 5000 and
 * are more than 60 years of age -  are eligible for covid vaccination drive.
 *
 */

package com.durgesh.software.sparkExercises

import com.durgesh.software.utils.CommonFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 *  written by Durgesh Agnihotri - 1614636968
 */
object covidVaccineEligibiliy extends CommonFunctions with App {

  // to only print error messages - if any
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = getSparkSession("covaxin eligibility")
  import spark.implicits._
  val inDF = spark.read.option("delimiter","|").option("inferSchema","true").option("header","true").csv("src/main/resources/employee")

  val ageDF = inDF.withColumn("age", datediff(current_date(),  to_date(col("birth_date")))/365)
  ageDF.show(false)
  ageDF.printSchema()

  val eligibilityDF = ageDF.filter($"age" > 60 && $"salary" < 50000 )
  eligibilityDF.show(false)

}
