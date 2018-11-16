package day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/29
  * 自定义排序
  */
object Ordereds {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ordereds").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int, Int, Int)] = sc
      .parallelize(List(("Linda", 20, 165, 1)
        , ("杨幂", 32, 168, 2)
        , ("苍老师", 32, 156, 4)
        , ("柳岩", 36, 162, 3)))
    val sorted: RDD[(String, Int, Int, Int)] = rdd.sortBy(t => {
      new Names(t._2, t._3, t._4)
    })

    println(sorted.collect().toBuffer)


  }

}

class Names(val i1: Int, val i2: Int, val i3: Int) extends Ordered[Names] with Serializable {//序列化或者样例类(case class)
  override def compare(that: Names): Int = {
    if (this.i1 == that.i1) {
      that.i2 - this.i2
    } else if (that.i2 == this.i2) {
      that.i3 - this.i3
    } else {
      that.i1 - this.i1
    }
  }
}
