package day17

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * creat by newforesee 2018/10/15
  * SparkStreaming入门Word Count
  */
object SparkStreamingWC {
  def main(args: Array[String]): Unit = {
    //分配至少两个线程,一个用于接收一个用于计算
    val conf: SparkConf = new SparkConf().setAppName("streamingwc").setMaster("local[2]")
    //创建SparkStreaming执行入口
    //设置时间间隔,时间间隔提交批次也就是batch
    val ssc = new StreamingContext(conf,Seconds(3))
    //首先,创建输入DStream,代表一个数据源比如kafka,socket
    //米持续不断的处理实时流数据
    //socketTextStream( )方法接收两个基本参数，第一个是监听哪个主机上的IP
    //第二个是监听哪个端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master",9999)
    //到这里为止可以理解为DStream每每秒封装一次RDD那也就是这3秒的RDD
    //接下来开始接收数据并计算使用sparkcore算子,只想操作在DStream中完成
    val word: DStream[String] = lines.flatMap(_.split(" "))
    val tuples: DStream[(String, Int)] = word.map((_,1))
    val result: DStream[(String, Int)] = tuples.reduceByKey(_+_)
    //到此为止实现了wc程序
    //每秒汇总发送指定socket端口上数据,都会被DStream接收到
    //然后开始一行一行的处理
    //然后调用一系列算子操作,知道生成最后一个WordCountRDD作为输出
    //此时就得到每秒发送过来的单词统计数据了
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
