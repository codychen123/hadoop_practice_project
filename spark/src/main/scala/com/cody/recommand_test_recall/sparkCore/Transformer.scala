package com.cody.recommand_test_recall.sparkCore

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Transformer {
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

    val data= List("I LOVE BIG DATA","YOU LOVE MACHINE learning","big data is importatnt for company")
    // 创建rdd
    val data_rdd = spark.sparkContext.parallelize(data)
//    data_rdd.take(1).foreach(println)
//    data_rdd.map(x=>x.split(" ")).foreach(x=>println(x.mkString("-")))
//    data_rdd.flatMap(_.split(" ")).foreach(x=>println(x.size))
//    data_rdd.flatMap(_.split(" ")).filter(_.size>4).take(3).foreach(println)
//    val rdd1 =spark.sparkContext.parallelize(Array(1,2,3,4,5,6),2)
//    rdd1.mapPartitions(_.map(_+1)).collect().take(10).foreach(println)

//    val rdd5 = spark.sparkContext.parallelize(List(1,2,3,4,5,6),3)
//    val rdd6 = spark.sparkContext.parallelize(List(1,3,4,8,9,11),3)

//    val rdd7 = rdd5.union(rdd6)
//    rdd7.map(x=>(print(x)))

    //pair rdd
    val x = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 4),("a", 2),
      ("b", 2), ("b",11),("b", 1), ("b", 10),("c", 1),("c", 5),("c", 6)
    ), 3)
    //    reduceByKey是transaction   reduce是action
//    x.reduceByKey(_+_).foreach(println)
//    x.reduceByKey((x,y)=>x+y)
    //  grouoByKey
//    x.groupByKey().take(3).foreach(println)
//    x.groupBy(_._2).take(2).foreach(println)
    //分组求和
    x.groupByKey().map(x=>(x._1,x._2.sum)).take(3)

    //排序
    x.groupByKey().map(x=>(x._1,x._2.sum)).sortBy(_._2,false).take(3)

    val movie  = spark.sparkContext.textFile("/ua.base")
    movie.map(x=>x.split("\t")).map(x=>(x(0),(x(1),x(3).toLong))).
      groupByKey().
      map(x=>(x._1,x._2.toArray.sortBy(x=>x._2).reverse)).
      take(3)

    movie.map(x=>x.split("\t")).map(x=>(x(0),(x(1),x(3).toLong))).
      groupByKey().
      map{
        x=>(x._1,x._2.toArray.sortBy(x=>x._2).reverse.take(10))
      }.take(10)

    //mapvalues,对pari中的，每个values 进行操作

    x.mapValues(x=>x+x)
    x.flatMapValues(x=>(x to 5))

    val rdd7 = spark.sparkContext.makeRDD(Array(("a", 1), ("a", 4),("a", 2),
      ("b", 2), ("b",11),("b", 1),("c", 1),("c", 5),("c", 6)))

    val rdd8 = spark.sparkContext.makeRDD(Array(("a", 1), ("a", 4),("a", 2),
      ("b", 2), ("b",11),("b", 1),("c", 1),("c", 5),("c", 6)))


    rdd7.join(rdd8)


    //fliter
    rdd7.filter{case (key,value)=>value>3}

    val input = spark.sparkContext.parallelize(Array(1,2,3,4,5,6,7,8),2)

    input.persist(StorageLevel.DISK_ONLY)
    input.persist(StorageLevel.MEMORY_ONLY_SER)
    // sample、cartesian
    val rdd_sample  = spark.sparkContext.parallelize(1 to 1000,4)
    rdd_sample.sample(true,0.4)


    //    val list1RDD = spark.sparkContext.parallelize(List("A","B"))
    //    val list2RDD = spark.sparkContext.parallelize(List(1,2,3))
    //    list1RDD.cartesian(list2RDD).take(3)

    //    coalesce、repartition、repartitionAndSortWithinPartitions
    //    分区数由多  -》 变少 coalesce
    val li = List(1,2,3,4,5,6,7,8,9)
    val s1=spark.sparkContext.parallelize(li,3)

    val s2=spark.sparkContext.parallelize(li,3).
      coalesce(1)

    //replication  进行重分区，解决的问题：本来分区数少  -》 增加分区数
    val li1 = List(1,2,3,4)
    val listRDD = spark.sparkContext.parallelize(li1,1)
    listRDD.repartition(2).foreach(println(_))

    //repartitionAndSortWithinPartitions
    //    repartitionAndSortWithinPartitions函数是repartition函数的变种，
    //    与repartition函数不同的是，repartitionAndSortWithinPartitions在给定的partitioner内部进行排序，性能比repartition要高。
    //    例子1. 将rdd数据中相同班级的学生分到一个partition中，并根据分数降序排序
    //
    //    例子2. 相同组合Key分组到同一分区，分区中先按照KEY排序，KEY相同的情况下按照其他键进行排序
    val repar_list = List(1, 4, 55, 66, 33, 48, 23)
    val repar_RDD = spark.sparkContext.parallelize(repar_list,2)


    //    cogroup、sortBykey、aggregateByKey

    //corgroup
    //    对两个RDD中的KV元素，
    //    每个RDD中相同key中的元素分别聚合成一个集合。
    //    与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
    val sc =spark.sparkContext
    val li1_cor = List((1, "苹果"), (2, "香蕉"))
    val li2_cor = List((1, "栗子"), (2, "苹果"), (3, "桃子"))
    val li3_cor = List((1, "西瓜"), (2, "甜瓜"), (3, "拿铁"))

    val list1RDD_cor = sc.parallelize(li1_cor)
    val list2RDD_cor = sc.parallelize(li2_cor)
    val list3RDD_cor = sc.parallelize(li3_cor)

    list1RDD_cor.cogroup(list2RDD_cor,list3RDD_cor)

    //    sortBykey
    //    sortByKey函数作用于Key-Value形式的RDD，
    //    并对Key进行排序。它是在org.apache.spark.rdd.OrderedRDDFunctions中实现的，实现如下
    val listss = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
    sc.parallelize(listss).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
    //aggregateByKey函数：

    //    对PairRDD中相同的Key值进行聚合操作，
    //    在聚合过程中同样使用了一个中立的初始值。
    //    和aggregate函数类似，aggregateByKey返回值
    //    的类型不需要和RDD中value的类型一致。因为aggregateByKey
    //    是对相同Key中的值进行聚合操作，所以aggregateByKey'
    //    函数最终返回的类型还是PairRDD，对应的结果是Key和聚合后的值，
    //    而aggregate函数直接返回的是非RDD的结果。
    val rdd9 = spark.sparkContext.parallelize(List((1,3),(1,2),(1,4),(2,3)),2)

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a:String,b:String):String={
      println("combOp: "+a+"\t"+b)
      a+b
    }
    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a:String,b:Int):String={
      println("SeqOp:"+a+"\t"+b)
      a+b
    }

    val aggregateByKeyRDD=rdd9.aggregateByKey("100")(seqOp, combOp)

    //cogroup、sortBykey
    val list1 = List((1, "www"), (2, "bbs"))
    val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
    val list3 = List((1, "com"), (2, "com"), (3, "good"))
    val list1RDD = spark.sparkContext.parallelize(list1)
    val list2RDD = spark.sparkContext.parallelize(list2)
    val list3RDD = spark.sparkContext.parallelize(list3)

    list1RDD.cogroup(list2RDD,list3RDD).foreach(tuple =>
      println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3))

    val list = List((99, "a"), (96, "b"), (66, "v"), (98, "d"))
    spark.sparkContext.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }
}
