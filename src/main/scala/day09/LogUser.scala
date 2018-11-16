package day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/28
  * 在一定时间范围内求用户所在基站停留时间top2
  */
object LogUser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogUser").setMaster("local")
    val sc = new SparkContext(conf)
    val files: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/mobilelocation/log")
    //切分用户数据
    val lacAndPhoneAndTime: RDD[((String, String), Long)] = files.map(line => {
      //切分
      val user = line.split(",")
      val phone: String = user(0)
      //手机号
      val time = user(1).toLong
      //时间戳
      val lac: String = user(2)
      //基站ID
      val enventType = user(3).toInt
      val time_long = if (enventType == 1) -time else time
      ((phone, lac), time_long)
    })
    //用户在相同基站停留时间总和
    val sumTime: RDD[((String, String), Long)] = lacAndPhoneAndTime.reduceByKey(_+_)
    //为了方便,需要整合用户在基站停留的时间信息,把基站ID放在前面
    val lacAndPhonAndTime = sumTime.map(t => {
      val phone = t._1._1
      //获取手机号
      val lac = t._1._2
      //获取基站
      val time = t._2 //获取用户在单个基站停留时长
      (lac, (phone, time))
    })
    //lacAndPhonAndTime
    //获取基站信息(经纬度)
    val lacInfo: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/mobilelocation/lac_info.txt")
    val lacAndXY = lacInfo.map(t => {
      val filds = t.split(",")
      val lac = filds(0)
      //获取基站ID
      val x = filds(1)
      //获取经度
      val y = filds(2) //纬度
      (lac, (x, y))
    })
    //把经纬度信息join到用户访问信息中去
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAndPhonAndTime.join(lacAndXY)
    //把数据整合方便计算
    val phoneAndTimeAndXY = joined.map(t => {
      val phone = t._2._1._1
      //手机号
      val lac = t._1
      //获取基站
      val time = t._2._1._2
      //时长
      val xy: (String, String) = t._2._2 //经纬度
      (phone, time, xy)
    })
    //按照手机号分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] = phoneAndTimeAndXY.groupBy(_._1)
    //然后按照时长排序
    val sorted: RDD[(String, List[(String, Long, (String, String))])] = grouped.mapValues(_.toList.sortBy(_._2).reverse)
    val result: Array[(String, List[(String, Long, (String, String))])] = sorted.take(2)
    //ArrayBuffer((18101056888,List((18101056888,97500,(116.296302,40.032296)), (18101056888,54000,(116.304864,40.050645)), (18101056888,1900,(116.303955,40.041935)))), (18688888888,List((18688888888,87600,(116.296302,40.032296)), (18688888888,51200,(116.304864,40.050645)), (18688888888,1300,(116.303955,40.041935)))))
    println(result.toBuffer)
//    result.foreach(t => {
//      println("手机号")
//      println(t._1)
//      println()
//    })
    sc.stop()
  }

}
