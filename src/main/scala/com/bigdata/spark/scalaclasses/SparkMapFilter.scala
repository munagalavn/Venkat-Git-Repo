package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkMapFilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkMapFilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
/*val nums=1 to 10 toArray
  val nrdd=sc.parallelize(nums)
  val res=nrdd.map(x=>x*x).filter(x=>x<20)
  res.collect.foreach(println)*/
    val data ="C:\\BigData\\Datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    //select * from tab where city=...
    //select city, count(*) from tab group by city....reducebykey or groupByKey ..
    //select join two tables
    // val res = aslrdd.filter(x=>x.contains("blr"))
    val skip = aslrdd.first()
// select * from tab where state=city and age<30
    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2),x(3))).filter(x=>x._3=="blr" && x._2<30)

    //array(jyo,12,blr,fri)
    //array(koti,29,blr,saturday)
res.collect.foreach(println)
    spark.stop()
  }
}
// spark 4 apis
/*
rdd api ... unstructure/structure data... programming friendly
datafram api ..structure/semi strucutre data ...prog/database friendly
dataset api ....any type structure/unstructure/semi/live structure data
dstream api ... only data live data
map : apply a logic on top of each and every element ...number of input and output elements length must be same
..uduslly spply s lohi on yop og only specific col use map ... array convert to tuple use map
filter: based on bool value (true) ... apply a logic on top of each & every element ...
//its almost select * from tab where ...
flatmap: apply a logic on top of each and every element next flatter results
flatmap = map + flatten
//usecase unsstructure data, apply a logic on all columns ...

 */