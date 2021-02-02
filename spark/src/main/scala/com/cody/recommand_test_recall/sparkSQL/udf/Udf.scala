package com.cody.recommand_test_recall.sparkSQL.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Udf extends UserDefinedAggregateFunction{
  //输入参数的类型
  override def inputSchema: StructType = StructType(StructField("inputColumn",LongType)::Nil)
  //聚合函数的缓冲结构，如下，定义用于记录累计值和累加数的字段结构
  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
  }
  //聚合函数返回值的数据类型
  override def dataType: DataType = DoubleType
  //是否始终在相同输入上返回相同输出
  override def deterministic: Boolean = true
  //buffer聚合缓冲器本事是一个row对象，可以调用buffer内的元素，如在索引检索一个值  （get,getBooleam() getlong）
  //也可以跟新索引的值，buffer内的array map对象是不可变的
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }
  //该函数负责将input代表的输入数据更新到buffer聚合缓存器中，buffer 缓冲器记录着累计和（buffer（0），和累计计数器buffer(1)）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)){
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getLong(1)+1
    }
  }
  //合并两个缓存器
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }
  //计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble/buffer.getLong(1)

}
