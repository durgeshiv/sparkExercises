package com.durgesh.software.sparkExercises

import com.durgesh.software.utils.CommonFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object prac1temp extends CommonFunctions with App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = getSparkSession("Sample code")

  case class fle(col1: Int, col2: String)
  import spark.implicits._
  val lines  = spark.read.csv("src/main/resources/test")
    .toDF("col1", "col2")
    .withColumn("col1", col("col1").cast("Int"))
    .withColumn("col2", col("col2").cast("Int"))
  lines.agg(sum("col1"), sum("col2")).show()
  println("doing same the rdd way")
  println(lines.rdd.map(_(1).asInstanceOf[Int]).reduce(_+_))
  //val enc = encoder
  //val sum: Dataset[fle] = lines.as(fle)
  println("dataset printing :: ")
  val ds = lines.as[fle]

  
  ds.printSchema()
  println(util.Properties.jdkHome)


  // Creating schema from a scala case class
  val schma = ScalaReflection.schemaFor[fle].dataType.asInstanceOf[StructType]
  //println(schma.treeString)
  println(schma)

  val cols = "sch1,sch2,sch3,sch4".split(",")
  val sch1 = cols.map( x=> StructField(x, StringType, false))
  val actualSchema = StructType(sch1)
  println("actual schema :: " + actualSchema)


  def fact(n: Int) : Int = {
    if (n <=1) 1
    else
    return n*(n-1)
  }

  case class Employ(name: String, age: Int, id: Int, department: String)

  val empData = Seq(Employ("A", 24, 132, "HR"), Employ("B", 26, 131, "Engineering"), Employ("C", 25, 135, "Data Science"))

  import spark.implicits._
  val empRDD = spark.sparkContext.parallelize(empData)
  val empDF = empRDD.toDF()


  println("printing DF")

  empDF.show()

  val empDS = empRDD.toDS()


  val filterDS = empDS.withColumn("ageFlag", when(col("age") > 24, "true").otherwise( "false"))  //filter( x => x.age > 24 )
  println("filter DS")
  filterDS.show()

  // Other way of creating dataset
  // 1. from DF by as-case class
  val empDS1 = empDF.as[Employ]

  // 2. from DF again
  val rddFromDF = empDF.rdd
  val newDF = spark.createDataset(empData)  //  createDataFrame(rddFromDF, empDF.schema)
  // 3. from rdd
  empRDD.toDS()

  val inpData = List((1,"emp1",10000,"IT"),(2,"emp2",8500, "IT"), (3,"emp3", 9300,"IT"),(4,"emp4", 4500,"HR"), (5,"emp5", 8500, "HR") )
  val df = spark.sparkContext.parallelize(inpData).toDF("id","name","sal","dept")

  import org.apache.spark.sql.functions._
  val avgSal = df.withColumn("avgSal", avg("sal").over(Window.partitionBy("dept")))
  println("print avg salary")
  avgSal.show()
  avgSal.filter(col("sal") > col("avgSal")).show()
}
