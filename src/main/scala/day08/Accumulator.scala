package day08

import org.apache.spark.{ SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    //创建一个累加器
    var sum = sc.accumulator(0)
    val RDD = sc.parallelize(Array(1,2,3,4,5,6))
    val result: Unit = RDD.foreach(f => {
      sum += f
    })
    println(result)

  }

}
