package day18

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * creat by newforesee 2018/10/16
  * 案例分析:过滤广告黑名单案例
  * 用户对我们的网站上的广告可以进行点击
  * 点击之后，是不是要进行实时计费，点一下，算一次钱
  * 但是，对于那些帮助某些无良商家刷广告的人，那么我们要有一个黑名单
  * 只要是黑名单中的用户点击的1告，我们就给他过滤掉
  *
  * Transform操作,可以实现我们的案例分析
  */
object BlackListFilter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BlackListFilter").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置黑名单
    val blackList: Array[(String, Boolean)] = Array(("Leo", true), ("jeck", true))
    //创建一个RDD
    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blackList)
    //使用socketTextStream来监听端口
    val socketData: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)
    //接下来将接受到的数据转换成和我们黑名单对应的数据格式
    //返回数据格式(username,date username)
    val users: DStream[(String, String)] = socketData.map(lines => (lines.split(" ")(1), lines))
    //使用sparkStreaming中的transform算子操作实现过滤


    val userRDDs: DStream[String] = users.transform(user => {
      //这里为什么不用jion?
      //如果当我们join的时候如果不是黑名单上的用户,就join不到了就会被剔除这样是不行的
      //所以我们要用左外链接进行join.这样join的话就不会丢数据就算join不到也会被保留
      val joinRDD: RDD[(String, (String, Option[Boolean]))] = user.leftOuterJoin(blackListRDD)
      //接下来调用filter算子实现join后的数据过滤
      val userFilter: RDD[(String, (String, Option[Boolean]))] = joinRDD.filter(tulpe => {
        //我们要取到数据的第二组数据的第二个元素进行过滤
        //也就是说当我们的元素为false的时候将不进行过滤
        if (tulpe._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      //将黑名单过滤后将处理真正的白名单数据
      val userRDD: RDD[String] = userFilter.map(t => t._2._1)
      userRDD

    })
    userRDDs.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
