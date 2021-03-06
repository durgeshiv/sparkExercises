package main.scala.com.durgesh.software.sparkExercises

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}

object cassandraRW extends App {

  val spark = SparkSession.builder()
    .appName("cassandra")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.cassandraFormat("books_by_author", "testkeyspace", "localhost").load()
  df.withColumn("status", lit("done"))
    .write
    .cassandraFormat("spark_cass", "testkeyspace", "localhost")
    .save()
}
