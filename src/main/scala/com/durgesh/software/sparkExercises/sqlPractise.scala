package com.durgesh.software.sparkExercises

import com.durgesh.software.utils.CommonFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, rank}

object sqlPractise extends CommonFunctions with App {


  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = getSparkSession("sql practise")
  val employeeDF = spark.read.option("header", "true").option("inferSchema","true")
    .csv("src/main/resources/employee_table.csv")

  //employeeDF.show(false)

  // third ighest salary

  val salDF = employeeDF
    .withColumn("rnk", dense_rank().over(Window.partitionBy("Region").orderBy(desc("Salary"))))
    .filter(col("rnk") <=5 )
    .select("Emp ID", "Salary", "Region", "rnk")
    //.drop("rnk")

  //println("Printing Dense Rank DF :  Salary / region  == 3rd Highest")
  println("Printing Dense Rank DF :  Salary / region  <=5")
  salDF.show(50, false)

  val salDF2 = employeeDF
    .withColumn("rnk", rank().over(Window.partitionBy("Region").orderBy(desc("Salary"))))
    .filter(col("rnk") <= 5)
    .select("Emp ID", "Salary", "Region", "rnk")

  println("Printing Dense Rank DF :  Salary / region  <=5")
//  println("Printing Rank DF :  Salary / region  == 3rd Highest")
  salDF2.show(50, false)

  // in sql
  employeeDF.createOrReplaceTempView("emp")
  println("sql query")
  spark.sql("select * from (select Salary,Region,rank() over (partition by Region order by Salary desc) as rnk from emp ) a where a.rnk <=5").show(false)
}
