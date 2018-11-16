package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cache").setMaster("local")
    val sc = new SparkContext(conf)
    //做持久化RDD操作
    //要用cache或者persist算子,当我们调用了textFile算子之后创建了RDd
    //我们需要调用cache或者persist才会生效,如果闷闷先创建了一个RDD,然后单独一行
    //执行cache()或者persist()算子,是没有用的比如:lines.cache()
    //会报错,大量文件会丢失
    val lines: RDD[String] = sc.textFile("path")
    val words: RDD[(String, Int)] = lines.map((_,1)).reduceByKey(_+_).cache()
    words.count()
    words.foreach(println)
    sc.stop()

  }

}
