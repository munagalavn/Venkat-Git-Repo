package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilteruc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mapfilteruc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "C:\\BigData\\Datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val skip = brdd.first()
    val res = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";"))
      .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5).toInt,x(6),x(7),x(8),x(9),x(10))).filter(x=>x._6>90000)
    res.take(9).foreach(println)

    /*val wcnt="C:\\BigData\\Datasets\\wcdata.txt"
    val crdd= sc.textFile(wcnt)
    //crdd.take(num = 2).foreach(println)
    val cnt=crdd.flatMap(x=>x.split(" ")).map(x=>x)
    cnt.collect.foreach(println)*/
import spark.implicits
    val don="C:\\BigData\\Datasets\\donation.txt"
    val drdd= sc.textFile(don)
    val head=drdd.first()
    val output="C:\\BigData\\Datasets\\output\\donationop"
    //crdd.take(num = 2).foreach(println)
//    val dnt=drdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).reduceByKey((a,b)=>a+b, 2).sortBy(x=>x._2, false,1).toDF("name","amt")
val dnt=drdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).groupByKey().map(x=>(x._1,x._2.sum))
    dnt.collect.foreach(println)
    //dnt.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
    dnt.saveAsTextFile(output)
    spark.stop()
  }
}
