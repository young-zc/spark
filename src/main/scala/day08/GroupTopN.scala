package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/27
  * 分组取TopN
  */
object GroupTopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GTopN").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Volumes/Untitled\\ 1/1000phone/spark/score.txt")
    val tuples: RDD[(String, Int)] = lines.map(t => {
      val str = t.split(" ")
      (str(0), str(1).toInt)
    })
    //按照班级进行分组
    val grouped: RDD[(String, Iterable[Int])] = tuples.groupByKey()
    val result: RDD[(String, Array[Int])] = grouped.map(t => {
      //先取到班级的名字
      val className = t._1
      val score: Array[Int] = t._2.toArray.sortWith(_ > _).take(3)
      (className, score)
    })
    result.foreach(t => {
      println(t._1)
      t._2.foreach(println)
    })
    sc.stop()
  }
}
