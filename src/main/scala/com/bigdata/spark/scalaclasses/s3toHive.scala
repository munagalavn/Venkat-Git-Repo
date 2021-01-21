package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._

// spark-submit --class com.bigdata.spark.scalaclasses.s3toHive --master local --deploy-mode client s3://venkatm2020/appjars/sparkpoc_2.11-0.1.jar s3://venkatm2020/input/csvdata/us-500.csv us500tab
object s3toHive {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkhive").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // dont forget to use enablehivesupport if u want to store in hive
    val sc = spark.sparkContext
    val data = args(0)
    val tab = args(1)
    val res = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    res.write.mode(SaveMode.Overwrite).saveAsTable(tab) // store data in hive
    res.printSchema()
    spark.stop()
  }
}
