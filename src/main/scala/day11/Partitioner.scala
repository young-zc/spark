package day11

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/30
  */
object Partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Partitioner").setMaster("local")
    val sc = new SparkContext(conf)
    val files = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/partitions.txt", 5)
    val tuples = files.map(t => {
      val url = new URL(t)
      //获取到学科名
      val host = url.getHost
      val str = host.substring(0, host.indexOf("."))
      (str, 1)

    })
    /**
      * 开始使用分区器
      */
      val partitioned = new Partitioned
    val results = tuples.reduceByKey(partitioned,_ + _)
    //没有使用自定义分区器会产生Hash碰撞,会导致某一个分区内的数据可能很多,某个分区内的数据少甚至没有
    results.saveAsTextFile("hdfs://master:9000/partitions2")

  }

}

class Partitioned extends Partitioner with Serializable {
  val map = Map("jave" -> 1, "ui" -> 2, "bigdata" -> 3, "android" -> 4, "h5" -> 5)

  override def numPartitions: Int = map.size + 1

  override def getPartition(key: Any): Int = {
    map.getOrElse(key.toString,0)
  }
}
