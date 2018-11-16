package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/27
  * spark 高级编程 二次排序
  * 1.按照文件中的第一列排序
  * 2.如果第一列相同则按照第二列排序
  */
object SortKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortKey").setMaster("local")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("/Volumes/Untitled\\ 1/1000phone/spark/sort.txt")
    val tuples = file.map(file => {
      val lines: Array[String] = file.split(" ")
      (Sorted(lines(0).toInt, lines(1).toInt), file)
    })
    val sorted = tuples.sortByKey()
    sorted.foreach(println)
    sc.stop()
  }

  case class Sorted(first:Int, first2:Int) extends Ordered[Sorted]{
    override def compare(that: Sorted): Int = {
      // 如果我们第一个值不相等的情况 下直接排序第 一个
      if(this.first - that.first != 0){
        this.first - that.first
      }else{
        //如果我們第一个值相等,那么排序第二个
        this.first2 - that.first2
      }
    }
  }

}
