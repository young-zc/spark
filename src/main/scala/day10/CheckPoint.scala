package day10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/29
  */
object CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CheckPoint").setMaster("local")
    val sc = new SparkContext(conf)
    //接下来进行检查点机制的创建
    sc.setCheckpointDir("hdfs://master:9000/CheckPoint")
    val files = sc.textFile("hdfs://master:9000/wc.txt")
    val words = files.flatMap(_.split(" "))
    val tuples = words.map((_,1))
    val result: RDD[(String, Int)] = tuples.reduceByKey(_ + _)  //官网建议我们在checkpoint的时候,先进行持久化RDD操作
    //也就是为了防止中间结果出现问题数据丢失,前面运行还没到checkpoint这一步那么我们
    //可以做一个cache,下次数据就直接就从cache中读取数据不需要从头计算,如果checkpoint执行成功,那么
    //前面的缓存将被销毁
    result.cache().checkpoint()
    result.foreach(println)
  }

}
