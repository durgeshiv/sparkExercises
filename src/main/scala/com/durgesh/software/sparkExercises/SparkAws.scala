package com.durgesh.software.sparkExercises


import com.durgesh.software.utils.CommonFunctions
import org.apache.spark.sql.SparkSession


object SparkAws extends CommonFunctions with App {


  val confMap = getConfig("src/main/resources/")

  val accessKey = confMap("accessKey")
  val secretKey = confMap("secretAccess")

  val spark = SparkSession.builder()
    .appName("spark-aws-test")
    .master("local[*]")
    .getOrCreate()

  //spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)

  spark.sparkContext.hadoopConfiguration.set("fs.s3a.buffer.dir", "/tmp/tempData")
 // spark.sparkContext.hadoopConfiguration.set("spark.sql.parquet.output.committer.class","org.apache.spark.sql.parquet.DirectParquetOutputCommitter")

  val df = spark.read.option("inferSchema","true").option("header","true")
    .csv("s3a://durgeshivinputbucket/crime_data/")

  df.show(2)
  println("dataframe count :: " + df.count())
  df.coalesce(1).write.partitionBy("year","month").option("header","true")
    .mode("append")
    .format("parquet").save("s3a://durgeshiv-spark-onprem-output/")

}