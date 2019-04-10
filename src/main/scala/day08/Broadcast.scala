package day08

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Broadcast").setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3
    //将factor做成广播变量让每个节点只有一份
    val broad: Broadcast[Int] = sc.broadcast(factor)
    val arrRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val lines: RDD[Int] = arrRDD.map((_: Int) * broad.value)
    lines.foreach(println)
    sc.stop()
  }

}
