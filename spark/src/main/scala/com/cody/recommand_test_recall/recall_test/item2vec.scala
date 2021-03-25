package com.cody.recommand_test_recall.recall_test

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.ml.linalg.Vector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions.{desc, row_number, udf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object item2vec {
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

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    import spark.implicits._
    val df = spark.sql("select * from badou.user_listen ")
    // userid,musicid,remaintime,durationhour
    /**
     * +--------------------+----------+----------+------------+
     * |              userid|   musicid|remaintime|durationhour|
     * +--------------------+----------+----------+------------+
     * |01e069ed67600f191...|4090309101|        15|          19|
     * |01d86fc1401b283d5...|6192809101|        75|          12|
     * */
//    df.show(10)
    val user_click_list = df.
      groupBy("userid").
      agg(collect_list("musicid").as("item_ids")).
      where(size(col("item_ids")).
        geq(2))
    user_click_list.show(3)
    /**
     * +--------------------+--------------------+
     * |              userid|            item_ids|
     * +--------------------+--------------------+
     * |0000cbb6ea39957f7...|[713100256, 92370...|
     * |0001658686d8fcd34...|[517009772, 51700...|
     * */

    // 实例化模型
    val word2Vec = new Word2Vec().
      setInputCol("item_ids").
      setOutputCol("word2vec_result").
      setVectorSize(16).
      setMinCount(0).
      setMaxIter(2).
      setSeed(1234)
    // 训练模型
    val word2VecModel = word2Vec.fit(user_click_list)
//    word2VecModel.getVectors.show(10,false)
    val vectorA = word2VecModel.
      getVectors.
      select(col("word").as("itemIdA"),col("vector").as("vectorA"))
    vectorA.cache()
//    vectorA.show()

    val vectorB = word2VecModel.
      getVectors.
      select(col("word").as("itemIdB"),col("vector").as("vectorB"))
    vectorB.cache()
//    vectorB.show()

    val crossDatas = vectorA.join(vectorB)
//    crossDatas.show()
    crossDatas.cache()

    //    计算相似性
    import org.apache.spark.sql.api.java.UDF2
    import org.apache.spark.sql.types.DataTypes
    spark.udf.register("vectorCosinSim", new UDF2[Nothing, Nothing, Double]() {
      @throws[Exception]
      override def call(vectora: Nothing, vectorb: Nothing): Double = Utils.cosineSimilarity(vectora, vectorb)
    }, DataTypes.DoubleType)

    val sim_item = crossDatas
      .withColumn(
        "cosineSimilarity", callUDF(
          "vectorCosinSim", col("vectorA"), col("vectorB")))
      .select("itemIdA", "itemIdB", "cosineSimilarity")
      .filter(col("itemIdA").notEqual(col("itemIdB")))
    sim_item.show(10)

    val windowSpec = Window.partitionBy("itemIdA").orderBy(col("cosineSimilarity").desc)
    val top_sim = crossDatas.withColumn("simRank", rank.over(windowSpec)).where(col("simRank").leq(200))
    val re = crossDatas
      .groupBy("itemIdA")
      .agg(
        collect_list("cosineSimilarity").as("columnSims"),
        collect_list("itemIdB").as("itemIds")
      ).select(
      col("itemIdA").as("item_id").cast(DataTypes.LongType),
      col("columnSims").as("column_sims").cast(DataTypes.StringType),
      col("itemIds").as("item_ids").cast(DataTypes.StringType)
    )
  }
}
