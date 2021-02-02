package com.cody.recommand_test_recall.sparkCore.offline

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hive.ql.optimizer.spark.SparkSkewJoinProcFactory.SparkSkewJoinJoinProcessor

object FuncTest {
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

    val df = spark.sql("select * from badou.orders")
    import spark.implicits._
    val orderNumberSort = df.select("user_id","order_number","order_hour_of_day")
      .rdd.map(x=>(x(0).toString,(x(1).toString,x(2).toString)))
      .groupByKey()
      .mapValues{
        _.toArray.sortWith(_._2<_._2)
      }.toDF("user_id","ons")

    // udf
    import org.apache.spark.sql.functions._
    val plusUDF = udf((col1:String,col2:String)=>col1.toInt+col2.toInt)
    df.withColumn("plus_udf",plusUDF(col("order_number"),col("order_dow"))).show()




  }
}
