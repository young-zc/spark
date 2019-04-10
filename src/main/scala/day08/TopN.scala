package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/27
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("/Volumes/Untitled\\ 1/1000phone/spark/top.txt")
    //进行数据提取
    val tuples: RDD[(Integer, String)] = lines.map(m => (Integer.valueOf(m),m))
    val result: Array[(Integer, String)] = tuples.sortByKey(ascending = true).take(3)
    result.foreach(t => println(t._2))
    sc.stop()

  }

}
