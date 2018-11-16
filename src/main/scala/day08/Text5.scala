package day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/29
  */
object Text5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Text5").setMaster("local")
    val sc = new SparkContext(conf)
    joinRdd(sc)
    sc.stop()
  }

  def joinRdd(sc:SparkContext): Unit ={
    val name = Array((1,"spark"),(2,"f"),(3,"h"),(4,"j"))
    val score = Array((1,100),(2,90),(3,80),(5,90))
    val namerdd = sc.parallelize(name)
    val scorerdd = sc.parallelize(score)
    val result = namerdd.join(scorerdd)
    result.collect.foreach(println)
  }
}
