package day09

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/28
  * 统计用户对每个学科各个模块的访问量
  */
object SubjectCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectCount").setMaster("local")
    val sc = new SparkContext(conf)
    val files: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/mobilelocation/access.txt")
    val tupleUrls: RDD[(String, Int)] = files.map(t => {
      val filds = t.split("\t")
      val url = filds(1)
      (url, 1)
    })
    //将相同的Url进行聚合,得到各个学科的访问量
    val sumed: RDD[(String, Int)] = tupleUrls.reduceByKey(_+_)
    //接下来获取学科信息,需要返回学科,url,访问量
    val subjectAndUrlAndCount: RDD[(String, String, Int)] = sumed.map(t => {
      val url = t._1
      //url
      val count = t._2
      //访问量
      val urls = new URL(url)
      val subject = urls.getHost //学科
      (subject, url, count)
    })
    //按照学科信息进行分组
    val grouped: RDD[(String, Iterable[(String, String, Int)])] = subjectAndUrlAndCount.groupBy(_._1)
    //在学科信息内部进行降序排序
    val sorted: RDD[(String, List[(String, String, Int)])] = grouped.mapValues(_.toList.sortBy(_._3).reverse)
    //取topN
    val result: Array[(String, List[(String, String, Int)])] = sorted.take(2)
    println(result.toBuffer)
    sc.stop()
  }

}
