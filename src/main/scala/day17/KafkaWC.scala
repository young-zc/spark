package day17

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * creat by newforesee 2018/10/15
  */
object KafkaWC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafkawc").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //首先配置kafka信息
    val zk = "master:2181,slave1:2181,slave2:2181"
    //给kafka消费者组
    val groupId = "gp1"
    //kafka的topic名字第一个参数是topic名字，第二个参数是线程数
    val topics: Map[String, Int] = Map[String,Int]("new"->1)
    //创建kafka的输入数据流
//    KafkaUtils.createStream(ssc,)
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,groupId,topics)
    //获取到的数据是键值对的格式
    val lines: DStream[String] = data.flatMap(_._2.split(" "))
    val words: DStream[(String, Int)] = lines.map((_,1))
    val result: DStream[(String, Int)] = words.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
