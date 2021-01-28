package com.cody.recommand_test_recall.sparkSQL

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object DataFrameDemo {
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

    val df = spark.sql("select uid,iid,cast (score as int) score,cast(ts as long) as ts from badou.movies")
    //    数据展示
//    df.show(10)
    //获取所有数据
    val col = df.collect()
    val col_list = df.collectAsList()
    // 获取所有信息
//    df.describe("score")
    //  first head take takeAsList
//    df.first()
//    df.head()
    //    df上的转化操作
    import spark.implicits._
    //过滤
//    df.where("score >3 and uid='1'").show(10)
//    df.filter("uid='3'").show(10)

    //查询指定列
//    df.select("uid", "iid").show(false)

//    df.select($"uid", $"score" + 1, $"ts" + 10)
//    df.select($"uid", $"score" + 1, $"ts").where($"score" > 3).orderBy(df("ts").asc).show(3)
//    df.select($"uid", $"score", $"ts")

//    df.select(df("uid") + 1)
    //可以对指定字段进行特殊处理
//    df.selectExpr("uid", "cast(iid as int)", "ts as ttts")

    //获取指定列
//    df.col("uid")
//    df.apply("iid")

    //去掉某一列
//    df.drop("uid")
    //排序操作,默认升序
//    df.sort($"ts")
//    df.orderBy($"ts")
//    df.orderBy($"uid", $"ts")

    //分组
    df.groupBy($"uid").count().orderBy($"count").show(10)
//    df.groupBy($"uid").mean("score")
    //    distinct dropDuplicates
//    df.distinct()
//    df.dropDuplicates()
    //agg 聚合操作

    df.groupBy("uid").agg("score" -> "mean", "iid" -> "max").show(3)
    //    uinon 格式一样的df
    df.limit(10).union(df.limit(100))
    //    join
    df.join(df, "uid")
    df.join(df, Seq("uid", "iid"))
    df.join(df, Seq("uid", "iid"), "left_outer")
    val df1 = df
    df.join(df1, df1("uid") === df("uid"))
    df.join(df1, df1.col("uid") === df.col("uid"))
    //    获取指定字段统计信息

    df.stat.corr("score", "ts")
    df.stat.cov("score", "ts")

    df.withColumnRenamed("uid", "user_id")
    df.withColumn("sss", df("uid"))

    df.join(df.groupBy("uid").mean("score"), "uid")


    val df2 = df.join(df.groupBy("uid").
      mean("score"), "uid").
      withColumnRenamed("avg(score)", "avg_score").withColumn("bais",   ($"score" - $"avg_score")*($"score" - $"avg_score"))

    // 空值处理
    //只要行数据有一个空值，直接drop掉


    df.drop()
    df.na.drop()
    df.na.drop(Array("uid"))
    //替换空

    df.na.fill(Map(("score","0")))
    //标准化




    import org.apache.spark.ml.feature.StandardScaler
    val df5 = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    val scaler = new StandardScaler().
      setInputCol("score").
      setOutputCol("score_stand").
      setWithStd(true).
      setWithMean(false)

    val model= scaler.fit(df5)
    model.transform(df5).show(4)
  }
}
