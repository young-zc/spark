package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action {
  def reduce(): Unit ={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6)
    val lines: RDD[Int] = sc.parallelize(arr)
    val sum = lines.reduce(_+_)
    println(sum)

  }
  def collect(): Unit ={
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6)
    val number = sc.parallelize(arr)
    val words = number.map(_*2)
    //注意:当我们调用了Action算子之后,那么这个RDD程序就结束了
    //之后再操作结果集就是scala操作了
    val result: Array[Int] = words.collect()
    for (elem <- result) {
      println(elem)
    }
  }
  def count(): Unit ={
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6)
    val number = sc.parallelize(arr)
    val counts = number.count()
    println(counts)
  }
  def take(): Unit ={
    val conf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6)
    val number = sc.parallelize(arr)
    val takes = number.take(3)
    println(takes.toBuffer)

  }
  def saveAsTextFile(): Unit ={
    val conf = new SparkConf().setAppName("count").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6)
    val number = sc.parallelize(arr)
   number.saveAsTextFile(" ")

  }
  def countByKey(): Unit ={
    val conf = new SparkConf().setAppName("countByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(("class1","leo"),("class2","linda"),("class3","newfor"),("class2","wety"),("class1","foresee"))
    val students = sc.parallelize(arr)
    val studentcounts: collection.Map[String, Long] = students.countByKey()
    println(studentcounts)


  }

  def main(args: Array[String]): Unit = {
   // reduce()
    //collect()
    //count()
    //take()
    countByKey()
  }
}
