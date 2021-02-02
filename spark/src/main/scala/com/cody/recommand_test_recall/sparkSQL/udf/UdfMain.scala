package com.cody.recommand_test_recall.sparkSQL.udf

import org.apache.spark.sql.SparkSession

object UdfMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession
      .builder().master("local")
      .appName("testRdd")
      .config("hive.metastore.uris",
        "thrift://"+"172.16.185.10"+":9083")
      .config("spark.sql.warehouse.dir","/spark-warehouse")
      //      .config("spark.storage.memoryFraction",0.6)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("avg_udf", Udf)
    val df = spark.sql("select uid,iid,cast (score as Long) score,cast(ts as long) as ts from badou.movies")
    df.createOrReplaceTempView("user")

    val result = spark.sql("select uid,avg_udf(score)  from user group by uid")
    result.show(3)
    //将udf转化为typedcolumn

    val avgs = AggregatorUdf.toColumn.name("avf_score")

    val res1 = df.select(avgs)
    res1.show(20)

  }


}
