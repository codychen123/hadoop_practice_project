package com.cody.recommand_test_recall.recall_test

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{LongType, StructField}

object RecallALS {
  case class temp_class(userId:String,uid:Int,iid:Int)
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
    import  spark.implicits._


    // 1.导入数据

    val data = process_data(spark)

    //  2.转化为intId

    val user_item_rating = transFormIdToInt(data,"userId","itemId",spark)

    //  3.切分训练和测试集
    val Array(training, test) = user_item_rating.randomSplit(Array(0.8, 0.2),seed = 1)   //将样本拆分为训练0.8，测试0.2

    //  3.训练模型

    val als = new ALS()    //定义一个ALS类
      .setMaxIter(20)        //迭代次数，用于最小二乘交替迭代的次数
      .setRegParam(0.01)    //惩罚系数
      .setRank(6)          //隐藏参数维度
      .setUserCol("uid")    //userid
      .setItemCol("iid")    //itemid
      .setRatingCol("rating")    //rating矩阵，这里跟你输入的字段名字要保持一致。很明显这里是显示评分得到的矩阵形式
    val model = als.fit(user_item_rating)

    //      这一步说白来，就是评测我这个模型的好坏。
    //    什么影响模型的好坏，不考虑其他模型，
    //lr,setMaxIter,setRank
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    //4.验证
    val predict_test = model.transform(test)

    val evals= new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evals.evaluate(predict_test)


    // 5.构造用户 物品 未评价df
    val predict_df = crateUserItemUnClick(user_item_rating,spark)



    // 预测结果
    val re = model.transform(predict_df)

    //推荐 top30
    val recommand =   re.selectExpr("userId","iid","prediction as score").withColumn("rank",
      row_number().over(Window.partitionBy("userId").orderBy($"score".desc))).
      filter(s"rank<=30").
      selectExpr("userId","iid as recall_list_channel_003","score as recall_weight_channel_003")

    recommand.show(30)

  }

  def inreaseId(df:DataFrame,spark: SparkSession,add_id:String):DataFrame={
    import  spark.implicits._
    val a = df.schema.add(StructField(add_id,LongType))

    val b = df.rdd.zipWithIndex()

    val c = b.map(tp => Row.merge(tp._1,Row(tp._2)))

    val new_df = spark.createDataFrame(c,a)
    new_df
  }
  def crateUserItemUnClick(user_item_rating:DataFrame,spark: SparkSession):DataFrame ={

    //    组合uid iid ，uid iid 是未曾出现的组合，然后我们按照topk给每个用户推出
    //在实际业务中，我们不会去选择全量物品，会基于用户标签过滤掉一部分物品
    //如在淘宝中，会过滤掉，用户购买金额，之类的。
    import  spark.implicits._

    val iid_hot= user_item_rating.groupBy("iid").count()

    val item_set = iid_hot.filter($"count">10).selectExpr("iid").rdd.map(x=>x(0)).collect().toSet

    val user_item_noClick = user_item_rating.rdd.map(x=>(x(0)+":"+x(1),(x(2),x(3)))).groupByKey().map{
      x=>
        val temp =  (x._1,x._2.toSet)
        //可以根据业务做优化。(item_set-temp._2)拿出用户未点击的物品
        // 用户消费金额最大值，（用户筛选，做了一个）进行过滤物品，
        //游艇 就不不需计算，

        val predict_item = (item_set-temp._2).take(20)

        val re = predict_item.map(x=>temp._1+":"+x)
        re
    }.flatMap(x=>x)
    val  predict_df = user_item_noClick.
      map(x=>(temp_class(x.split(":")(0),x.split(":")(1).toInt,x.split(":")(2).toInt))).
      toDF
    predict_df
  }
  def process_data(spark:SparkSession):DataFrame={
    import  spark.implicits._
    //    10w用户
    val user_profile = spark.sql("select * from badou.user_profile")
    val user_listen = spark.sql("select userid,musicid,cast(remaintime as double),cast(durationhour as double) from badou.user_listen")

    //     2.计算用户听某一首歌曲的总时间
    val itemTotalTime = user_listen.selectExpr("userId",
      "musicId", "cast(remainTime as double)",
      "cast(durationHour as double)").
      groupBy("userId", "musicId").
      sum("remainTime").
      withColumnRenamed("sum(remainTime)", "itemTotalTime")

    //    用户总共听歌时长
    val totalTime = user_listen.selectExpr("userId", "musicId",
      "cast(remainTime as double)",
      "cast(durationHour as double)").
      groupBy("userId").
      sum("remainTime").
      withColumnRenamed("sum(remainTime)", "totalTime")
    //    uid ,iid ,rating
    val data = itemTotalTime.join(totalTime, "userId").
      selectExpr("userId", " musicId as  itemId", " itemTotalTime/totalTime as rating")
    data
  }
  def transFormIdToInt(data: DataFrame,userId:String,itemId:String,spark:SparkSession):DataFrame={
    import  spark.implicits._
    val data_with_UserId = data.groupBy(userId).count().selectExpr(userId + " as userId1","count")
    val  temp_uid_data =   inreaseId(data_with_UserId ,spark,"uid")
    //为 iid 转化类型
    val data_with_ItemId = data.groupBy(itemId).count().selectExpr(itemId+" as itemId1","count")
    val  temp_iid_data =  inreaseId(data_with_ItemId,spark,"iid")
    //join 数据
    val all_data  = data.join(temp_uid_data,$"userId"===$"userId1").
      join(temp_iid_data,$"itemId"===$"itemId1").
      selectExpr("userId","uid","iid","rating")

    all_data

  }
//  val data_with_UserId = data.groupBy("userId").count().selectExpr("userId" + " as userId1","count")
  //  def find_best_param(als: ALS,traingData:DataFrame): Unit ={
  //
  //
  //    val paramGrid = new ParamGridBuilder()
  //      .addGrid(als.maxIter,Array(50,100,150))
  //      .addGrid(als.rank,Array(32,64,128,256))
  //      .build()
  //
  //    val evaluator = new RegressionEvaluator()
  //      .setMetricName("rmse")
  //      .setLabelCol("rating")
  //      .setPredictionCol("prediction")
  //
  //
  //    val trainValidationSplit = new TrainValidationSplit()
  //      .setEstimator(als)
  //      .setEvaluator(evaluator)
  //      .setTrainRatio(0.8)
  //      .setEstimatorParamMaps(paramGrid)
  //    val bestModel: TrainValidationSplitModel = trainValidationSplit.fit(traingData)
  //    bestModel
  //  }
}
