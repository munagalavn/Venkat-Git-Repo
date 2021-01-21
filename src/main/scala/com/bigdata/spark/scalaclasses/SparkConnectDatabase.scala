package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkConnectDatabase {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkConnectDatabase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //option1 to get data from any database
    val qry="(select * from abhidf where sal>2500) t"
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msdf = spark.read.format("jdbc").option("user","msuername")
      .option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable",qry).option("url",url).load()
    //df.show()
    msdf.createOrReplaceTempView("mstab")

//second way of reading data from db
    import java.util.Properties
    val oprop = new Properties()
    oprop.setProperty("user","ouser")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val ourl = "Jdbc driver"
    val odf = spark.read.jdbc(ourl,"table",oprop)
    odf.createOrReplaceTempView("otab")
    val join = spark.sql("select * from")
    spark.stop()
  }
}
