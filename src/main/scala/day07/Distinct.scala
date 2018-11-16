package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Distinct").setMaster("local")
    val sc = new SparkContext(conf)
    //做一个UV统计案例
    //uv : user view 每天每个用户可能对网站点击多次
    //此时需要对用户进行去重,然后统计每天有多少用户访问了网站
    //而不是所有用户访问多少次(pv)
    val arr = Array (
      "user1 2018-09-26 16:45:20",
      "user1 2018-09-26 16:45:21",
      "user1 2018-09-26 16:45:21",
      "user2 2018-09-26 16:45:22",
      "user2 2018-09-26 16:45:23",
      "user3 2018-09-26 16:45:23",
      "user2 2018-09-26 16:45:26",
      "user3 2018-09-26 16:45:27",
      "user4 2018-09-26 16:45:26",
      "user5 2018-09-26 16:45:28",
      "user5 2018-09-26 16:45:29",
      "user6 2018-09-26 16:45:30",
      "user6 2018-09-26 16:45:31"
    )
    val logs: RDD[String] = sc.parallelize(arr,2)
    val userRDD: RDD[String] = logs.map(t => {
      val lines = t.split(" ")
      lines(0)
    })
    val userCount: Long = userRDD.distinct().count()
    println(userCount)
  }

}
