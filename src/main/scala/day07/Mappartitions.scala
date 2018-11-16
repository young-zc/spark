package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Mappartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Mappartitions").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array("张三","李四","王五","赵六")
    val nameRDD = sc.parallelize(arr,2)
    val hashMap = mutable.HashMap("张三"->278.5,"李四"->290.0,"王五"->286.3,"赵六"->190.6)
    val stuScores: RDD[Double] = nameRDD.mapPartitions(m => {
      var list = mutable.LinkedList[Double]()
      while (m.hasNext) {
        val name = m.next()
        val Scores = hashMap.get(name)
        list ++= Scores
      }
      list.iterator
    })
    stuScores.collect.foreach(println)

  }

}
