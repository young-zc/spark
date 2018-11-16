package newforesee

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 的 wordcount实现
  */
object SparkWC {
  def main(args: Array[String]): Unit = {
    //创建sparkConf配置文件对象
    val conf = new SparkConf()
      //spark程序的名字
      .setAppName("sparkWC")
      //如果setMaster那么就是本地模式
      //local:用一个线程模拟spark集群运行程序
      //local[2]:用两个线程模拟集群运行程序
      //local[*]:用所有空闲的线程来模拟
      //.setMaster("local")
    //创建上下文也就是启动入口
    val sc = new SparkContext(conf)//然后我们可以进行读取数据处理数据
    //val lines: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt")
    val lines: RDD[String] = sc.textFile("hdfs://master:9000/wc.txt")
    //进行单词统计,首先调用进行切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //调用map算子进行单词计数
    val tuples: RDD[(String, Int)] = words.map((_,1))
    //调用reduceByKey算子,进行单词聚合
    //会根据key聚合value,也就是将相同key的value进行相加
    val reduce: RDD[(String, Int)] = tuples.reduceByKey(_+_)
    //将数据进行降序排序
    val sortWC: RDD[(String, Int)] = reduce.sortBy(_._2,ascending = false)
    //打印
    println(sortWC.collect.toBuffer)
    sortWC.saveAsTextFile("hdfs://master:9000/sparkWC2")
    //关闭资源
    sc.stop()
  }
}
