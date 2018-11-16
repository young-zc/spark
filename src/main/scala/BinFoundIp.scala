import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * creat by newforesee 2018/11/9
  */
object BinFoundIp {
  val arr = new ArrayBuffer[(String,String,String)]()

  //ip进行比较
  def compare(ipStart: Array[String], ipEnd: Array[String], ipSplit: Array[String]): Int = {
    for(i <- 0 until 4){
      if(ipSplit(i).toInt < ipStart(i).toInt){
        return -1
      }else if (ipSplit(i).toInt > ipEnd(i).toInt){
        return 1
      }
    }
    0
  }

  //二分查找
  def searchCode(ipSplit: Array[String], file: RDD[String]) : Unit = {
    file.foreach(x => {
      val fields: Array[String] = x.split("\t")
      //将start_ip,与code存储在arr中
      arr += ((fields(0),fields(1),fields(2)))
    })
    var low = 0
    var height: Int = arr.length - 1
    while (low <= height) {
      val m: Int = (low + height) >> 1 //移位运算符,向右移动一位
      val ipStart: Array[String] = arr(m)._1.split(".")
      val ipEnd: Array[String] = arr(m)._2.split(".")
      if (compare(ipStart,ipEnd,ipSplit) == 0 )
         arr(m)._3
      else if (compare(ipStart,ipEnd,ipSplit) < 0) height = m - 1
      else if (compare(ipStart,ipEnd,ipSplit) > 0) low = m + 1
    }
  }



  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("binFoundIp").setMaster("local")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("D://ip_rules")
    if(args.length > 0){
      //获取传入的ip参数
      val ip = args(0)
      //使用.将ip进行分割
      val ipSplit: Array[String] = ip.split(".")
      val code: Unit = searchCode(ipSplit,file)
    }
  }
}
