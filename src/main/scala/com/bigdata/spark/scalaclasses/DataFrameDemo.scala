package com.bigdata.spark.scalaclasses

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object DataFrameDemo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameDemo").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._ //rdd convert to dataframe .. df convert to rdd or dataset
    import spark.sql
    import org.apache.spark.sql.types._
     sc.setLogLevel("ERROR")
    /*val data = "file:///C:\\BigData\\Datasets\\bank-full.csv"
    val output="C:\\BigData\\Datasets\\output\\bankoutput"
    val rdd=sc.textFile(data)
    val head=rdd.first()
    val fields = head.split(";").map(x=> StructField(x.replaceAll("\"",""), StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = rdd.filter(x=>x!=head).map(x=>x.replaceAll("\"","").split(";")).map(x => Row.fromSeq(x))
    val df = spark.createDataFrame(rowRDD, schema)
    val all = df.count.toInt
    //df.show(4)
    df.createOrReplaceTempView("banktab")
    val res=spark.sql("select * from banktab where loan=\"yes\" and age>60")
    //res.show()
    res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)*/

   /*val data = "C:\\BigData\\Datasets\\bank-full.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",";")
      .option("inferSchema","true").load(data)
 df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    res.show()*/

    val data = "C:\\BigData\\Datasets\\us-500.csv"
    val output="C:\\BigData\\Datasets\\output\\us-500_dmn_nm_output"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",")
      .option("inferSchema","true").load(data)
 df.createOrReplaceTempView("tab")
  //  val res = spark.sql("select (SUBSTRING_INDEX(SUBSTR(email, INSTR(email, '@') + 1),'.',1)) domain_nm," +
    //                            " count(*) cnt from tab group by domain_nm order by cnt desc")
    //res.show()
    val cnt = df.count().toInt
   // df.show(8,false)
    //res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
   // val res = df.where(col("state")==="NY")
    val res = df.where($"state"==="OH")
    // as per sprk/scala "anything within double quote called string"
  //  val res = df.groupBy($"state").count().orderBy($"count".desc)
    res.show(5)

    spark.stop()
  }
}
