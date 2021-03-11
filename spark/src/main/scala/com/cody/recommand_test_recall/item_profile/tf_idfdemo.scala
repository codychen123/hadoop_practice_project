package com.cody.recommand_test_recall.item_profile

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature._

object tf_idfdemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("TopicExtraction")
      .getOrCreate()
    val soureceData= spark.createDataFrame(Seq(
      (0,"soyo spark like spark hadoop spark and spark like spark"),
      (1,"i wish i can like java i"),
      (2,"but i dont know how to soyo"),
      (3,"spark is good spark tool")
    )).toDF("label","sentence")


    //进行分词
    val tokenizer=new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData=tokenizer.transform(soureceData)
    wordsData.show(false)  //表示不省略,打印字符串的所有单词

    val numTerms=20000
    val countVectorizer=new CountVectorizer().
      setInputCol("words").setOutputCol("segFreqs")
      .setVocabSize(numTerms)
    val vocabModel=countVectorizer.fit(wordsData)
    val docTermFreq=vocabModel.transform(wordsData)
    docTermFreq.show(3)
    //使用HashingTF生成特征向量

    //使用CountVectorizer生成特征向量
    val idf=new IDF().setInputCol("segFreqs").setOutputCol("features")
    val idfModel=idf.fit(docTermFreq)
    val result=idfModel.transform(docTermFreq)
    result.show(false)

    result.cache()
    result.select("label","features").show(10)


    result.rdd.map(x=>x.getAs[org.apache.spark.ml.linalg.Vector]("features")).take(3)
  }
}
