package com.cody.recommand_test_recall.sparkCore

import org.apache.spark.sql.SparkSession

object Action extends App {
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

  val rdd_act1 = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9),2)
  //  reduce
  rdd_act1.reduce(_+_)


  //  collect
  rdd_act1.collect()
  //count
  rdd_act1.count()
  //first
  rdd_act1.first()

  //     aggregate函数
  //    将每个分区里面的元素进行聚合，然后用combine函数将每个分区
  //    的结果和初始值(zeroValue)进行combine操作。这个函数最终返回
  //    的类型不需要和RDD中元素类型一致。
  //    seqOp操作会聚合各分区中的元素，然后
  //    combOp操作把所有分区的聚合结果再次聚合，
  //    两个操作的初始值都是zeroValue.
  //    seqOp的操作是遍历分区中的所有元素(T)，
  //    第一个T跟zeroValue做操作，
  //    结果再作为与第二个T做操作的zeroValue，
  //    直到遍历完整个分区。combOp操作是把各分区聚合的结果，
  //    再聚合。aggregate函数返回一个跟RDD不同类型的值。因此，
  //    需要一个操作seqOp来把分区中的元素T合并成一个U，另外一个操作combOp把所有U聚合。
  //    过程大概这样：
  //    首先，初始值是(0,0)，这个值在后面2步会用到。
  //    然后，(acc,number) => (acc._1 + number, acc._2 + 1)，number即是函数定义中的T，这里即是List中的元素。所以acc._1 + number,acc._2 + 1的过程如下。
  //
  //    1.  0+1,  0+1
  //    2.  1+2,  1+1
  //    3.  3+3,  2+1
  //    4.  6+4,  3+1
  //    5.  10+5,  4+1
  //    6.  15+6,  5+1
  //    7.  21+7,  6+1
  //    8.  28+8,  7+1
  //    9.  36+9,  8+1
  val rdd_aggre = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9),2)


  rdd_aggre.aggregate(0)(_+_,_+_)
  rdd_aggre.aggregate(0)(
    (x,y)=>x+y,
    (par1,par2)=>par1+par2
  )


  val res = rdd_aggre.aggregate((0,0))(
    (acc,number) => (acc._1 + number, acc._2 + 1),
    (par1,par2) => (par1._1 + par2._1, par1._2 + par2._2)
  )
  res._1/res._2



  rdd_act1.foreachPartition(x=>x.foreach(println))
}
