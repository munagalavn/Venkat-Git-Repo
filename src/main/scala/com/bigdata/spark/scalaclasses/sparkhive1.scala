package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._

// spark-submit --class com.bigdata.spark.scalaclasses.sparkhive --master local --deploy-mode client s3://venkatm2020/appjars/sparkpoc_2.11-0.1.jar s3://venkatm2020/input/csvdata/us-500.csv us500tab
object sparkhive1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkhive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // dont forget to use enablehivesupport if u want to store in hive
    val sc = spark.sparkContext
    val data = args(0)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res= spark.sql("select * from tab where state='NY'")
    res.show(5)
    val tab = args(1)
    val msurl="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)
    res.write.mode(SaveMode.Overwrite).saveAsTable(tab) // store data in hive
    res.printSchema()
    spark.stop()
  }
}
