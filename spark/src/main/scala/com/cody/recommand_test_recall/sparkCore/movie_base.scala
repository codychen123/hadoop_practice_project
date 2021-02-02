package com.cody.recommand_test_recall.sparkCore

import org.apache.spark.sql.SparkSession

object movie_base {
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

    val df =spark.sql("select * from badou.movies")
    val rdd_df= df.rdd
    df.show(10)


  }
}
