package com.cody.recommand_test_recall.recall_test
import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object RecallUserCF {
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
     *1.导入数据，并加工评分数据
     */
    //    10w用户
    //我们在日常spark开发中，90%的数据都是从hive里面读取的。
    //user_listen有500mb, user_listen 的 rdd的partition有多少个：
    // block128mb, 4个。当你从hdfs里面读取数据，你的rdd的分区是有 block来决定的
    val user_profile = spark.sql("select * from badou.user_profile")
    val user_listen = spark.sql("select userid,musicid,cast(remaintime as double),cast(durationhour as double) from badou.user_listen ")

    // 2.计算用户听一首歌的总时间
    val itemTotalTime = user_listen.selectExpr("userid","musicid", "cast(remaintime as double)","cast(durationHour as double)").
      groupBy("userid", "musicid").
      sum("remaintime").
      withColumnRenamed("sum(remaintime)","itemTotalTime")
    //+--------------------+----------+---------+
    //|              userid|   musicid|totalTime|
    //+--------------------+----------+---------+
    //|00941e652b84b9967...|6326709127|    200.0|
    //|01e7b27c256cc06c1...|5652309207|    280.0|
    //|0115519e4f2e7488d...| 536400214|    195.0|
    //+--------------------+----------+---------+
    val totalMusicTime = user_listen.selectExpr("userid","musicid", "cast(remaintime as double)","cast(durationHour as double)").
      groupBy("userid").
      sum("remaintime").
      withColumnRenamed("sum(remaintime)","totalMusicTime")
    //    uid ,iid ,rating
    //    itemTotalTime
    //|              userId|   musicId|itemTotalTime|
    //+--------------------+----------+-------------+
    //|00941e652b84b9967...|6326709127|        200.0|
    //|01e7b27c256cc06c1...|5652309207|        280.0|
    //|0115519e4f2e7488d...| 536400214|        195.0|
    //+--------------------+----------+-------------+
    //    totalTime 表比较小，1000w (1gb)
    //+--------------------+---------+
    //|              userId|totalTime|
    //+--------------------+---------+
    //|001182ecc8be5e709...|     75.0|
    //|003bfa68b61dd5c9e...|   1810.0|
    //|00c68f637da1a8731...|    763.0|
    //+--------------------+---------+

    // 在做join的是后，提前聚合数据.
    val data = itemTotalTime.
      join(totalMusicTime,"userid").
      selectExpr("userid","musicid as itemid","itemTotalTime/totalMusicTime as rating")
    data.createOrReplaceTempView("user")
    data.cache()
    /*
    *
    * +--------------------+----------+--------------------+
|              userid|    itemid|              rating|
+--------------------+----------+--------------------+
|00941e652b84b9967...|6326709127|  0.0855431993156544|
|01e7b27c256cc06c1...|5652309207| 0.12641083521444696|
|0115519e4f2e7488d...| 536400214|                0.26|
|00dd865af9144dc9b...| 131100221|                 1.0|
|001ffc595663bfd86...| 545200221| 0.04143646408839779|
    * */
    /***
     *2.计算用户相似性
     */
    //(x1*y1+x2*y2......)/|X|*|Y|
    //|X| =sqrt(x1^2+x2^2+x3^2)
    //|Y| =sqrt(y1^2+y2^2+y3^2)
    //    2.1计算分母
    import spark.implicits._
    //  userSumPowRatin  每个用户 对所有的物品进行评分的平方和 （|X| =sqrt(x1^2+x2^2+x3^2)
    val userSumPowRating =  data.
      rdd.
      map(x => (x(0).toString, x(2).toString)).
      groupByKey().
      // 一条数据
      // (003d60470fbc5eb862940b4a9c8b3b26,CompactBuffer(0.6582343830665979, 0.24109447599380485, 0.10067114093959731))
      mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum)).
      toDF("userId", "sqrt_rating_sum")

    userSumPowRating.cache()
    userSumPowRating.show(3)

    //    2.计算分子
    //    uid 247999，如果要计算 两两相似性，直接爆炸。
    val data_with_copy =data.selectExpr("userId as userId1","itemId as itemId1","rating as rating1")

    //     item->user倒排表,去除一样用户的打分
    //    todo优化计算空间，对用户不进行笛卡尔积
    //    优化前
    //     333499378条数据
    val user_item2item = data.
      join(data_with_copy,data_with_copy("itemId1")===data("itemId")).
      filter("userId <> userId1 ")
    user_item2item.selectExpr("userId").distinct().count()
    //+--------------------+----------+--------------------+--------------------+----------+--------------------+
    //|              userid|    itemid|              rating|             userId1|   itemId1|             rating1|
    //+--------------------+----------+--------------------+--------------------+----------+--------------------+
    //|000082424fec0235d...|2826109193| 0.05610630668635309|000082424fec0235d...|2826109193| 0.05610630668635309|
    //|0000a7b03e50ba378...| 170400319| 0.03088911839263919|0000a7b03e50ba378...| 170400319| 0.03088911839263919|
    //|0000a7b03e50ba378...|5252909338| 0.08309078959722092|0000a7b03e50ba378...|5252909338| 0.08309078959722092|
    //|00014ca835551c0c0...| 660800353|                 0.5|00014ca835551c0c0...| 660800353|                 0.5|
    //    92964 说明 部分用户跟其他人听的歌没有任何交集，这部分用户不会有召回结果，所以，我们需要对这部分用户以其他方式进行相似性计算。
    //    计算两个用户在一个item下的评分的乘积，consine举例的分子的一部分
    //    dot
    import org.apache.spark.sql.functions._

    val selectData = user_item2item.selectExpr("userId","rating","itemId","userId1","rating1")

    val product_udf = udf((s1:Double,s2:Double)=>s1*s2)

    //    val user_data =  selectData.selectExpr("userId","userId1","rating1*rating as rating_product")
    val user_data =  selectData.
      withColumn("rating_product",product_udf(col("rating"),col("rating1"))).
      selectExpr("userId","userId1","itemId","rating_product")


    //    此处直接爆炸了
    val user_rating_sum =  user_data.
      groupBy("userId","userId1").
      agg("rating_product"->"sum").
      withColumnRenamed("sum(rating_product)","rating_dot")

    //    分子除以分母 做相似性

    val userSumPowRatingCopy = userSumPowRating.selectExpr("userId as userId1","sqrt_rating_sum as sqrt_rating_sum1")

    val df_sim = user_rating_sum.
      join(userSumPowRating,"userId").
      join(userSumPowRatingCopy,"userId1").
      selectExpr("userId","userId1",
        "rating_dot/(sqrt_rating_sum*sqrt_rating_sum1) as cosine_sim")

    //      2.  获取相似用户的物品集合
    //       2.1取得前n个相似用户
    val df_nsim =df_sim.
      rdd.
      map(x=> (x(0).toString,(x(1).toString,x(2).toString))).
      groupByKey().
      mapValues { x =>
        x.toArray.sortWith((x, y) => x._2 > y._2).slice(0,10)}.
      flatMapValues(x=>x).toDF("userid","user_v_sim").
      selectExpr("user_id","user_v_sim._1 as user_v","user_v_sim._2 as sim")

    //    2.2获取用户的物品集合进行过滤,拿到每一个用户的物品集合，列表
    val df_user_item = data.
      rdd.
      map(x=>(x(0).toString,x(1).toString+"_"+x(2).toString)).
      groupByKey().mapValues(x=>x.toArray)
      .toDF("user_id","item_rating_arr")
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
    val df_gen_item = df_nsim.join(df_user_item,"user_id")
      .join(df_user_item_v,"user_v")
    //       2.3用一个udf过滤相似用户user_id1中包含user_id已经打过分的物品


    val filter_udf = udf{(items:Seq[String],items_v:Seq[String])=>
      val fMap = items.map{x=>
        val l=x.split("_")
        (l(0),l(1))
      }.toMap
      items_v.filter{x=>
        val l = x.split("_")
        fMap.getOrElse(l(0),-1) == -1
      }
    }

    val df_filter_item = df_gen_item.withColumn("filtered_item",
      filter_udf(col("item_rating_arr"),col("item_rating_arr_v")))
      .select("user_id","sim","filtered_item")


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
     * */


    //        2.4公式计算 相似度*rating
    val simRatingUDF = udf{(sim:Double,items:Seq[String])=>
      items.map{x=>
        val l = x.split("_")
        l(0)+"_"+l(1).toDouble*sim
      }
    }
    val itemSimRating = df_filter_item.withColumn("item_prod",
      simRatingUDF(col("sim"),col("filtered_item")))
      .select("user_id","item_prod")

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
        "cast(split(item_prod,'_')[1] as double) as score")

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
