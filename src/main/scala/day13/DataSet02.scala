package day13

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSet02 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataSet02").master("local").getOrCreate()
    //导入隐式转换
    import spark.implicits._
    val df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    //将分布式存储在集群上的分布式数据集全部收集到Driver
//    df.collect().foreach(println)
    //println(df.count())
    //first 获取数据中的第一条
    //println(df.first())
    //遍历所有数据如果我们在集群上运行的话会看不到全部结果
    //因为分布式运行只有调用collect才可以 一般不会这么做 会oom 内存溢出
    //df.foreach(println(_))
    //reduce对数据进行聚合
    //println(df.map(t => 1).reduce(_ + _))
    //show 打印显示
    df.take(2).foreach(println)
    spark.stop()
  }

}
