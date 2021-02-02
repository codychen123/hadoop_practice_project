package com.cody.recommand_test_recall.sparkSQL.udf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

//定义出啊被纳入聚合函数的数据，buffer缓冲器，以即返回结果的泛型参数
object AggregatorUdf  extends  Aggregator[User,AvgScore,Double]{
  //定义聚合的灵芝，满足任何k+zero=k
  override def zero: AvgScore =AvgScore(0L,0L)
  //定义作为avg独享的buffer，如何处理每一条数据的聚合逻辑，与update差不多，每次调用reduce都会更新buffer聚合缓冲器的值
  override def reduce(b: AvgScore, a: User): AvgScore ={
    b.sum+=a.score
    b.count+=1
    b
  }
  //合并两个缓冲器
  override def merge(b1: AvgScore, b2: AvgScore): AvgScore ={
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }
  //定义结果的逻辑，reduction表示经过多次reduce merge之后的结果，avg对象记录所有数据的累计和，累计数
  override def finish(reduction: AvgScore): Double = {
    reduction.sum.toDouble/reduction.count
  }
  //指定中间值的编码器类型
  override def bufferEncoder: Encoder[AvgScore] = Encoders.product
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
