package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparktest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparktest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\BigData\\Datasets\\asl.csv"
    val df = spark.read.format("csv").option("header","true").load(data)
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select * from tab ")
val audf = udf(ageoffer _)
    spark.udf.register("offer", audf)
    val dudf = udf(days _)
    spark.udf.register("days",dudf)
    val res = spark.sql("select *,offer(age) todayoffer from tab ").withColumn("day", dudf($"day"))
   // val res = spark.sql("select city, count(*) cnt from tab group by city order by cnt desc")
    res.show()

    spark.stop()
  }
  def ageoffer(age:Int) = {
    if (age > 13 && age < 30) "30% off"
    else if(age>=30 && age<=59) "20% off"
    else if (age>=60) "40% off"
    else "no offer"
  }
  def days(day:String) = day match {
    case "sun" | "sunday" =>1
    case "mon" | "monday"=>2
    case "tue" | "tuesday"=>3
    case "wed" | "wednesday"=>4
    case "thu" | "thursday"=>5
    case "fri" | "friday"=> 5
    case "sat" | "saturday"=>6
    case _ => 0
  }

}
