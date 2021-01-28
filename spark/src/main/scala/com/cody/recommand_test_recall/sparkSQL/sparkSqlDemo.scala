package com.cody.recommand_test_recall.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object sparkSqlDemo {
  def main(args: Array[String]): Unit = {

    //    spark SQL是spark的一个模块，主要用于进行结构化数据的处理。它提供的最核心的编程抽象就是DataFrame。
    //    在Spark中，DataFrame是一种以RDD为基础的分布式数据集，
    //    类似于传统数据库中的二维表格。DataFrame与RDD的主要区别在于，
    //    前者带有schema元信息，即DataFrame所表示的二维表数据集的每一列都带有名称和类型。
    //    这使得Spark SQL得以洞察更多的结构信息，
    //    从而对藏于DataFrame背后的数据源以及作用于DataFrame之上的变换进行了针对性的优化，
    //    最终达到大幅提升运行时效率的目标。反观RDD，由于无从得知所存数据元素的具体内部结构，
    //    Spark Core只能在stage层面进行简单、通用的流水线优化。

    val spark = SparkSession
      .builder().master("local")
      .appName("testRdd")
      .config("hive.metastore.uris",
        "thrift://"+"172.16.185.10"+":9083")
      .config("spark.sql.warehouse.dir","/spark-warehouse")
      //      .config("spark.storage.memoryFraction",0.6)
      .enableHiveSupport()
      .getOrCreate()
    //
//    val spark1 = SparkSession
//      .builder().master("local1")
//      .appName("testRdd1")
//      .config("hive.metastore.uris",
//        "thrift://"+"192.168.163.10"+":9083")
//      .config("spark.sql.warehouse.dir","F:/recommend/spark-warehouse")
//      .config("spark.storage.memoryFraction",0.6)
//      .enableHiveSupport()
//      .getOrCreate()
    //    rdd 转化dataframe
    //目的是把ua.base这张hdfs上的数据，变成df。
    //   1 通过 case class 创建 DataFrames（反射）
    case class User(uid:String,iid:String,score:Int,ts:Long)
    //第一：case 加在class前  可以自动生成equal，hascode，tostring，copy，apply
    //    val u1  = User("1","1",1,1l)


    val data     = spark.sparkContext.textFile("/ua.base")
//    val rdd_data =  data.map(x=>x.split("\t")).map(x=>User(x(0),x(1),x(2).toInt,x(3).toLong))
    val res = data.map(x=>x.split("\t")).filter(x=>(x(2).toInt)>3).map(x=>(x(0),x(1))).groupBy(x=>x._1)
      .mapValues(x=>x.size)
//    val rdd_data_case = data.
//      map(x=>x.split("\t")).
//      map(x=>(x(0),x(1),x(2).toInt,x(3).toLong))
    //方便rdd和dataframe之间的转化
    import spark.implicits._

//    val movie_df=rdd_data_case.toDF()
    // 方式二：通过 structType 创建 DataFrames（编程接口）

//    val data1 = spark.sparkContext.textFile("/ua.base")



    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row
    //    Dataframe 的概念有点像传统数据库中的表，每一条记录都代表了一个 Row Object.
//    val rowRDD:RDD[Row] =data1.map(
//      line=>{
//        val temp =line.split("\t")
//        Row(temp(0),temp(1),temp(2).toInt,temp(3).toLong)
//      }
//    )

//    val rdd_data_cases = data.map(x=>x.split("\t")).map(x=>(x(0),x(1),x(2).toInt,x(3).toLong))
//    val structType: StructType = StructType(
//      StructField("uid", StringType, true) ::
//        StructField("iid", StringType, true) ::
//        StructField("scsore", IntegerType, true) ::
//        StructField("ts", LongType, true) ::Nil
//    )

//    val df: DataFrame = spark.sqlContext.createDataFrame(rowRDD,structType)

//    df.show(10)

//    df.map(x=>x.split("\t"))
    //    方式三：通过 json 文件创建 DataFrames

//    val data2 = spark.sqlContext.read.json("/ua.json")
//    data2.createOrReplaceTempView("people")
//    spark.sqlContext.sql("select * from people").show()

    //    创建一个临时视图以即全局临时试图 切记实在一个application中
//    movie_df.createOrReplaceTempView("user")
    //    movie_df.createGlobalTempView("user_global") Spark 2.1.0版本中引入了Global temporary views 。



    //DataFrame的read和save和savemode
    //读取1
//    val data2_json = spark.sqlContext.read.json("/ua.json")
//    val data2_par = spark.sqlContext.read.parquet("/ua.parquet")
    //读取2
//    val data3_json=spark.sqlContext.read.format("json").load("/ua.json")
//    val data3_par=spark.sqlContext.read.format("parquet").load("/ua.parquet")
    //方式三，默认是parquet格式
//    val df5 = spark.sqlContext.load("E:\\666\\users.parquet")


    //数据的保存
//    movie_df.write.json("/ua.json")
//    movie_df.write.parquet("/ua.parquet")
//
//    movie_df.write.save("/ua_data")

    //spark sql
    //    测试全局试图
//    spark1.sql("select * from global_temp.user_global")
//    spark.sql("select uid,count(1) from user group by uid")
//    spark.sql("select uid,row_number() over(partition by uid order by ts)as rk from user group by uid")
//    spark.sql("select iid,count(*) from user group by iid")


  }
}