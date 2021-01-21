package com.bigdata.spark.scalaclasses
import com.bigdata.spark.scalaclasses.importall._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object sparkudf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//val data = "C:\\BigData\\Datasets\\bank-full.csv"
val data = "C:\\BigData\\Datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load(data)
    //df.show()
    df.createOrReplaceTempView("tab")
   // val res = spark.sql("select * from tab where balance>70000 and marital='married'")
    //val res = df.where($"balance">=60000 && $"marital"==="married") //domain specific language
    //val res = spark.sql("select *, concat(job,'_',marital,'_',education) fullname, concat_ws(' ',job,marital,education) fullname1 from tab")
   // val res = df.withColumn("fullname", concat_ws("_",$"marital",$"job",$"education"))
   //   .withColumn("fullname1",concat($"marital", lit("-"),$"job", lit("_"),$"education"))
  //  val res = df.withColumn("job",regexp_replace($"job","-",""))
     // .withColumn("marital",regexp_replace($"marital","single","bachelor"))
     // .withColumn("marital",regexp_replace($"marital","divorced","separated"))
    //  .withColumn("marital",when($"marital"==="married","couple")
   //   .when($"marital"==="single","bachelor")
  //    .when($"marital"==="divorced","separated").otherwise($"marital"))
   // val res = df.groupBy($"marital").agg(count("*").alias("cnt")).orderBy($"cnt".asc)

//val res = df.groupBy($"state").count().orderBy($"count".desc)
//val res = df.groupBy($"state").agg(max($"zip").as("cnt"))
//val res =spark.sql("select state, city, collect_list(first_name) allnames from tab group by state, city")
  //  val res = df.groupBy($"state",$"city").agg(collect_list($"first_name").alias("names"))


    val uf = udf(offer _) //spark don't know anything but support udf.. convert function to udf
    spark.udf.register("off", udf(offer _)) // in spark sql must convert udf to a registered name
    //val res = df.withColumn("weekendoffer",uf($"state"))
    val res = spark.sql("select *, off(state) monthoffer from tab")
    res.show(15,false)
   // res.printSchema()


    spark.stop()
  }
}
