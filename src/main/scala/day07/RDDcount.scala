package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计文件每次出现的单词个数
  */
object RDDcount {

  def main(args: Array[String]): Unit = {
    //创建一个sparkConf
    val conf = new SparkConf().setAppName("count").setMaster("local")
    //上下文 即执行入口
    val sc = new SparkContext(conf)
    //开始统计单词
    val lines: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt")
    val words: RDD[(String, Int)] = lines.map((_,1))
    val result: RDD[(String, Int)] = words.reduceByKey(_+_)
    result.foreach(print)

  }
}
