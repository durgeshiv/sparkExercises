package com.durgesh.software.sparkExercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType

object cca1 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

/*
    val indf = spark.read.format("avro").load("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q1\\*")

    indf.select("order_id","order_status").write.format("parquet").option("compression", "gzip").save("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q1\\output\\")
    indf.count()*/

    import org.apache.spark.sql.functions._

   /* val custDF = spark.read.csv("F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q2\\customers\\")
      .select(col("_c0").as("customer_id"), col("_c1").as("customer_fname"))
    val ordersDF = spark.read.csv("F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q2\\orders\\")
      .select(col("_c2").as("customer_id"), col("_c3").as("order_status"))
      .filter(col("order_status") === "COMPLETE")
      .groupBy("customer_id")
      .count.where("count >4")
      .join(custDF, "customer_id")
      .sort("count")
      .show()
*/

/*

    val productDF = spark.read.parquet("F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q3\\")
      .groupBy("product_category_id")
      .max("product_price").as("max_price")
      .show()
    
*/

    /**
     *
     *
     * Q4
     * Tab delimited data
     * Find all customer that live in city  "Caguas"
     * output schema should be :: customer_id: Integer, customer_name: String, customer_city: String
     * output in avro
     * should be saved with deflate format
     *
     */

   /* val inDF = spark.read.option("delimiter", "\t").csv("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q4\\")
      .toDF("customer_id", "customer_name", "customer_city")
      .withColumn("customer_id", col("customer_id").cast(IntegerType))

    inDF.filter(col("customer_city") === "Caguas").write.format("avro").option("compression", "deflate")
      .save("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q4\\output")*/

    /**
     *Q5
     * avro to Tab -delimited in BZIP2
     * output be like , customer_id, customer_name(only firt3 characters), customer_lname
     */
   /* val inDF = spark.read.format("avro")
      .load("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q5\\")
      .toDF("customer_id","customer_name","customer_lname")
      .withColumn("customer_name", substring(col("customer_name"), 0,3))
      .write
      .format("csv")
      .option("delimiter", "\t")
      .option("compression", "bzip2")
      .save("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q5\\output")

*/

    /**
     * Q6
     */

    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql._
    import org.apache.spark.sql.cassandra

    val spark  = SparkSession.builder()
      .appName("Cassandra connector")
      .master("local[*]")
     // .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
      //.config("spark.cassandra.connection.host", "localhost")
     // .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .getOrCreate()

    /*val connector = CassandraConnector(spark.sparkContext.getConf)
    connector.withSessionDo(session =>
      {
        session.execute("DROP KEYSPACE IF EXISTS testkeyspace")
        session.execute("CREATE KEYSPACE testkeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("USE testkeyspace")
        session.execute("CREATE TABLE emp(emp_id int PRIMARY KEY,emp_name text,emp_city text,emp_sal varint,emp_phone varint)")
        session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(1,'John', 'London', 0786022338, 65000);")
        session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(2,'David', 'Hanoi', 0986022576, 40000);")
        session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(3,'John Cass', 'Scotland', 0786022342, 75000);")
        session.execute("INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal) VALUES(4,'Bob Cass', 'Bristol', 0786022258, 80950);")
      })*/

/*

    val df = spark.read.cassandraFormat("emp","testkeyspace", "localhost:9042").load()
    df.show()
*/
/*

    val df = spark.read.format("parquet").load("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q7\\")
      //.withColumn("month", month(from_unixtime(col("order_date")/1000)))
      //.withColumn("year", year(from_unixtime(col("order_date")/1000)))
      .withColumn("order_date", to_date(from_unixtime(col("order_date")/1000)))
      .filter(col("order_date").rlike("2014-03") && col("order_status") === "pending_payment".toUpperCase)
      .groupBy("order_date").count
      .select(col("order_date"), col("count").as("pending_orders"))
      .orderBy("order_date")
      .write
      .format("json")
      .save("file:///F:\Github\sparkExercises\src\main\resources\mock1\q7\output")
*/

    // Q8
   /* val custDF = spark.read.format("avro").load("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q8\\customers\\")
      .select("customer_id", "customer_fname","customer_lname")
    val ordDF = spark.read.format("avro").load("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\mock1\\q8\\orders\\")
      .select(col("order_date"), col("order_status"), col("order_customer_id").as("customer_id"))
    custDF.join(ordDF, "customer_id")
      .withColumn("order_date", to_date(from_unixtime(col("order_date")/1000)))
      .filter(col("order_status") === "COMPLETE" && col("order_date").contains("2013"))
      .groupBy("customer_fname", "customer_lname").count()
      .select(col("customer_fname"), col("customer_lname"),col("count").as("orders_count"))
      .show()
*/
    val crimeDF = spark.sparkContext.textFile("file:///F:\\Github\\sparkExercises\\src\\main\\resources\\regexrep")
    print(" Match non digits :: ")
    crimeDF.map(x => x.replaceAll("[\\D]", "")).foreach(println)
    print(" Match all digits :: ")
    crimeDF.map(x => x.replaceAll("[\\d]", "")).foreach(println)
    print(" Match word characters :: ")
    crimeDF.map(x => x.replaceAll("[\\w]", "")).foreach(println)
    print(" Match non word characters :: ")
    crimeDF.map(x => x.replaceAll("[\\W]", "")).foreach(println)
    print(" Match test 1:: ")
    crimeDF.map(x => x.replaceAll("[A-Za-z]", "")).foreach(println)
    print(" Match test 2:: ")
    crimeDF.map(x => x.replaceAll("[^A-Za-z]", "")).foreach(println)

  }


}
