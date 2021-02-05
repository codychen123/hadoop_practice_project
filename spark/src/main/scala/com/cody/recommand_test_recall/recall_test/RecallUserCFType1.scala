package com.cody.recommand_test_recall.recall_test

import org.apache.spark.sql.SparkSession

object RecallUserCFType1 {
  def main(args: Array[String]): Unit = {
    //    在driver端创建 sparksession
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

    /***
     *1.导入数据，并加工数据
     */
    //    10w用户
    val user_profile = spark.sql("select * from badou.user_profile")
    val user_listen = spark.sql("select userid,musicid,cast(remaintime as double),cast(durationhour as double) from badou.user_listen")

    user_profile.show(3)

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
      selectExpr("userId", "musicId as itemId", "itemTotalTime/totalTime as rating")



    data.createOrReplaceTempView("user")
    //        data.show(2)
    /**
     *
     * +--------------------+---------+-------------------+
     * |              userId|   itemId|             rating|
     * +--------------------+---------+-------------------+
     * |000005a451226ca84...|177200319|                1.0|
     * |0000cbb6ea39957f7...|068800255|0.20651420651420652|
     * +--------------------+---------+-------------------+
     */
    //    data.write.mode("overwrite").saveAsTable("user_item_rating")

    /***
     *2.计算用户相似性
     */
    import spark.implicits._

    //    2.1计算分母

    val user_sum_rating_withCluster = data.
      rdd.
      map(x => (x(0).toString, x(2).toString)).
      groupByKey().
      mapValues(x => math.sqrt(x.toArray.map(rating => math.pow(rating.toDouble, 2)).sum)).
      toDF("userId", "sqrt_rating_sum")


    //    2.2计算分子

    //    val tmp = data.map(x => (x(0).toString, x(2).toString))
    //        tmp.show(3)
    //       |a|= sqrt(a1^2+a2^2+...+an^2)

    val data_with_userProfile =  data.
      join(user_profile,"userId").
      selectExpr("userId",
        "itemId",
        "gender",
        "age",
        "salary",
        "location",
        "rating")

    val data_with_userProfile_copy = data_with_userProfile.
      selectExpr("userId as userId1",
        "gender as gender1",
        "salary as salary1",
        "itemId as itemId1",
        "age as age1",
        "location as location1",
        "rating as rating1")

    //此处相当于 两个用户如果性别相似 年龄相似 停留时段相似，且点击了一样的物品才参与计算。
    val user_item2item_withCluster = data_with_userProfile.
      join(data_with_userProfile_copy,
        data_with_userProfile("itemId")===data_with_userProfile_copy("itemId1") and
          data_with_userProfile("gender")===data_with_userProfile_copy("gender1") and
          data_with_userProfile("age")===data_with_userProfile_copy("age1") and
          data_with_userProfile("salary")===data_with_userProfile_copy("salary1") and
          data_with_userProfile("location")===data_with_userProfile_copy("location1")
      ).
      filter("userId1 <> userId ")

    //     6674332数据,67885用户有交几,33316没有交集。
    //    67885 说明跟他同年龄 短的 同性别的用户，有三万多没有交集，那这部分用户该如何处理呢。有一个方式
    //    基于 用户的聚类信息，这两万个用户可以基于 聚类信息找到与他在同类簇中相似的用户，加入用户表，
    //    这块当我们讲了聚类之后，我们再来合并。
    //分子计算完毕
    val user_Product_sum_withCluster =  user_item2item_withCluster.
      selectExpr("userId","userId1","itemId","rating1*rating as rating_product").
      groupBy("userId","userId1").
      agg("rating_product"->"sum").
      withColumnRenamed("sum(rating_product)","rating_sum_pro")


    val userSumRatingWithClusterCopy = user_sum_rating_withCluster.
      selectExpr("userId as userId1","sqrt_rating_sum as sqrt_rating_sum1")


    val  u1_u2_rating = user_Product_sum_withCluster.join(user_sum_rating_withCluster,"userId").
      join(userSumRatingWithClusterCopy,"userId1")


    val user_sim = u1_u2_rating.selectExpr("userId","userId1",
      "rating_sum_pro/(sqrt_rating_sum*sqrt_rating_sum1) as user_sim")

    user_sim.createOrReplaceTempView("user_sim")


    //以上可以每周算一次，然后存hive，用的时候直接
    //val user_sim = spark.sql("select * from badou.sim")

    //      2.  获取相似用户的物品集合
    //       2.1取得前n个相似用户
//    val df_nsim  = spark.sql("select userId ," +
//      "userId1," +
//      "user_sim, " +
//      "row_number() over(partition by userId order by user_sim desc) " +
//      "as rk from user_sim " +
//      "having rk <10").selectExpr(" userId as user_id","userId1 as user_v","user_sim as sim")
    val df_nsim = spark.sql("select * from (select userId,userId1,user_sim,row_number() " +
      "over(partition by userId order by user_sim desc) as rk from user_sim)t1 where rk < 10").selectExpr(" userId as user_id","userId1 as user_v","user_sim as sim")
    df_nsim.cache()
    /**
     * +--------------------+--------------------+---+
     * |             user_id|              user_v|sim|
     * +--------------------+--------------------+---+
     * |000005a451226ca84...|00293ac91b13c06b4...|1.0|
     * |000005a451226ca84...|01a91693461650b82...|1.0|
     * |000005a451226ca84...|01452d2d3266778ba...|1.0|
     * +--------------------+--------------------+---+
     * */

    // 2.2获取用户的物品集合进行过滤,拿到每一个用户的物品集合，列表
    val df_user_item = data.rdd.map(
      x=>(x(0).toString,x(1).toString+"_"+x(2).toString)).
      groupByKey().mapValues(x=>x.toArray).
      toDF("user_id","item_rating_arr")
    /**
     * +--------------------+--------------------+
     * |             user_id|     item_rating_arr|
     * +--------------------+--------------------+
     * |01c2e8315cba77f4b...|    [5691509338_1.0]|
     * |003d60470fbc5eb86...|[445700349_0.6582...|
     * |0103456cd157aa167...|    [0135609257_1.0]|
     * +--------------------+--------------------+
     * */
    val df_user_item_v = df_user_item.selectExpr("user_id as user_v",
      "item_rating_arr as item_rating_arr_v")
    /** df_gen_item:
     * +------+-------+-------------------+--------------------+--------------------+
        |user_v|user_id|                sim|     item_rating_arr|   item_rating_arr_v|
        +------+-------+-------------------+--------------------+--------------------+
        |   296|     71|0.33828954632615976|[89_5, 134_3, 346...|[705_5, 508_5, 20...|
        |   467|     69| 0.4284583738949647|[256_5, 240_3, 26...|[1017_2, 50_4, 15...|
        |   467|    139|0.32266158985444504|[268_4, 303_5, 45...|[1017_2, 50_4, 15...|
        |   467|    176|0.44033327143526596|[875_4, 324_5, 32...|[1017_2, 50_4, 15...|
        |   467|    150|0.47038691576507874|[293_4, 181_5, 12...|[1017_2, 50_4, 15...|
        +------+-------+-------------------+--------------------+--------------------+
     * */

    val df_gen_item = df_nsim.join(df_user_item,"user_id").
      join(df_user_item_v,"user_v")
    //  2.3用一个udf过滤相似用户user_id1中包含user_id已经打过分的物品
    import org.apache.spark.sql.functions._

    //返回一些列item，这些item是不存在在uid 已经打分的物品中。
    val filter_udf = udf{(items:Seq[String],items_v:Seq[String])=>
      //把每一行的 自身点击的item_rating ，让如Map词典中
      val fMap = items.map{x=>
        //        89_5  -- 89,5
        val l=x.split("_")
        (l(0),l(1))
      }.toMap
      //    去相似用户点击的物品列表里面，剔除哪些改用已经点击的物品
      items_v.filter{x=>
        val l = x.split("_")
        fMap.getOrElse(l(0),-1) == -1
      }
    }

    val df_filter_item = df_gen_item.withColumn("filtered_item",
      filter_udf(col("item_rating_arr"),col("item_rating_arr_v"))).
      select("user_id","sim","filtered_item")


    /** df_filter_item:
     * +-------+-------------------+--------------------+
     * |user_id|                sim|       filtered_item|
        +-------+-------------------+--------------------+
        |     71|0.33828954632615976|[705_5, 508_5, 20...|
        |     69| 0.4284583738949647|[762_3, 264_2, 25...|
        |    139|0.32266158985444504|[1017_2, 50_4, 76...|
        |    176|0.44033327143526596|[1017_2, 762_3, 2...|
        |    150|0.47038691576507874|[1017_2, 762_3, 2...|
        +-------+-------------------+--------------------+
      71    78    0.3333   [705_5, 508_4]
      71    89    0. 44    [405_5, 408_5]
       uid=71 ,item_id=705, 0.3333*5
      uid=71 ,item_id=508, 0.3333*4
      uid=71 ,item_id=405, 0.4444*5
      uid=71 ,item_id=408, 0.4444*5
     * */


    //        2.4公式计算 相似度*rating
    val simRatingUDF = udf{(sim:Double,items:Seq[String])=>
      items.map{x=>
        val l = x.split("_")
        l(0)+"_"+l(1).toDouble*sim
      }
    }
    val itemSimRating = df_filter_item.withColumn("item_prod",
      simRatingUDF(col("sim"),col("filtered_item"))).
      select("user_id","item_prod")

    /**itemSimRating:
     *+-------+--------------------+
        |user_id|           item_prod|
        +-------+--------------------+
        |     71|[705_1.6914477316...|
        |     69|[762_1.2853751216...|
        |    139|[1017_0.645323179...|
        |    176|[1017_0.880666542...|
        |    150|[1017_0.940773831...|
        +-------+--------------------+
     */


    val userItemScore = itemSimRating.select(itemSimRating("user_id"),
      explode(itemSimRating("item_prod"))).toDF("user_id","item_prod")
      .selectExpr("user_id","split(item_prod,'_')[0] as item_id",
        "cast(split(item_prod,'_')[1] as double) as score").
      selectExpr("user_id as userId","item_id as recall_list_channel_003 " ,"score as recall_weight_channel_003")


    /**userItemScore:
     *+-------+-------+------------------+
        |user_id|item_id|             score|
        +-------+-------+------------------+
        |     71|    705|1.6914477316307988|
        |     71|    508|1.6914477316307988|
        |     71|     20|1.6914477316307988|
        |     71|    228| 1.353158185304639|
        |     71|    855|1.6914477316307988|
        +-------+-------+------------------+
     */
  }
}
