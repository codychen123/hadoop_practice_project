package com.cody.recommand_test_recall.sparkCore.repartitionAndSort

import org.apache.spark.{SparkConf, SparkContext}

object User {
  case class UserKey(area: String, spend: Int)

  object UserKey {
    implicit def orderingBy[A <: UserKey]: Ordering[A] = {
      Ordering.by(so => (so.area, so.spend * -1))
    }
  }
  import org.apache.spark.Partitioner

  class UserPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val userkey = key.asInstanceOf[UserKey]
      Math.abs(userkey.area.hashCode()) % numPartitions
    }
  }

  def main(args: Array[String]) {


    //定义hdfs文件索引值
    val area_idx: Int = 0
    val user_idx: Int = 1
    val spend_idx: Int = 2

    //定义转化函数，不能转化为Int类型的，给默认值0
    def safeInt(s: String): Int = try {
      s.toInt
    } catch {
      case _: Throwable => 0
    }

    //定义提取key的函数
    def createKey(data: Array[String]): UserKey = {
      UserKey(data(area_idx), safeInt(data(spend_idx)))
    }

    //定义提取value的函数
    def listData(data: Array[String]): List[String] = {
      List(data(area_idx), data(user_idx), data(spend_idx))
    }


    def createKeyValueTuple(data: Array[String]): (UserKey, List[String]) = {
      (createKey(data), listData(data))
    }



    //设置master为local，用来进行本地调试
    val conf = new SparkConf().setAppName("user_partition_sort").setMaster("local")
    val sc = new SparkContext(conf)

    //学生信息是打乱的
    val student_array = Array(
      "上海,01,5129",
      "上海,02,6549",
      "北京,04,684",
      "北京,05,792",
      "上海,03,7453",
      "北京,01,65128",
      "广州,04,1452",
      "深圳,01,9561",
      "深圳,04,6512",
      "广州,08,3654",
      "广州,09,7453",
      "深圳,07,4821"
    )
    //将学生信息并行化为rdd
    val user_rdd = sc.parallelize(student_array)
    //生成key-value格式的rdd
    val user_rdd_map = user_rdd.map(line => line.split(",")).map(createKeyValueTuple)
    //根据StudentKey中的grade进行分区，并根据score降序排列
    val res = user_rdd_map.repartitionAndSortWithinPartitions(new UserPartitioner(3))
    //打印数据
    res.collect.foreach(println)
  }
}
