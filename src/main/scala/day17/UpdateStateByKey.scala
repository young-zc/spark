package day17

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * creat by newforesee 2018/10/15
  * 单词累计.累加批次结果
  */
object UpdateStateByKey {

  /**
    * (Iterator[ (K, Seq[V]，Option[S] )]) => Iterator[ (K, S) ]
    * //第一个参数，是我们从kafka获取到的元素，Key, String 类型
    * //第二个参数，使我们进行单词计数统计的value值，Int类型
    * //第三个参数，使我们每次批次提交的中间结果集
    */
  val updateFunc: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (ite:Iterator[(String,Seq[Int],Option[Int])])=>{
    ite.map(t=>{
      (t._1,t._2.sum+t._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("bykey").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("hdfs:master:9000/checkpoint")
    val zk = "master:2181,slave:2181,slave2:2181"
    //kafka消费者组
    val groupId = "gp2"
    val topics: Map[String, Int] = Map[String,Int]("t"->1)

    //创建kafka输入数据流,来获取kafka中的数据
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,groupId,topics)
    //处理数据但数据是(k,v)
    val lines: DStream[String] = data.flatMap((_: (String, String))._2.split(" "))
    val words: DStream[(String, Int)] = lines.map((_: String,1))
    //开始使用UpdateStateByKey
    val value: DStream[(String, Int)] = words.updateStateByKey(
      updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      rememberPartitioner = true
    )
    value.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
