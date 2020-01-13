package youngtest

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
  * 数据格式：
  *       用户:好友B,好友C,好友D,....
  *       A:B,C,D,F,E,N
  *       B:A,C,E,K
  *       C:F,A,D,I
  *       D:A,E,F,L
  * 需求：求每两个用户的共同好友
  */
object GetCommonFriends {

  def main(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession.builder().appName("GetCommonFriends").master("local").getOrCreate()
    val ssc: SparkContext = ss.sparkContext

    //读取数据
    val srcData: RDD[String] = ssc.textFile("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\commonfriends")
    srcData.foreach(println)

    //数据处理，切分       格式：Array(userA,(friendA,friendB,...))
    val splited: RDD[Array[String]] = srcData.map((_:String).split(":"))
    splited.foreach((line: Array[String]) => println("line(0):" + line(0) + "     line(1):" + line(1)))

    //数据处理，格式装换，把数据转换为(friendA,userA)的格式
    val transformed: RDD[(String, String)] = splited.flatMap {
      arr: Array[String] =>
        val user: String = arr(0)
        val friends: Array[String] = arr(1).split(",")
        var map: Map[String, String] = Map[String, String]()
        friends.foreach((friend: String) => map += (friend -> user))
        map
    }
    transformed.foreach(println)

    //聚合，把所有拥有某一friend的user全部聚合起来
    val reduced: RDD[(String, String)] = transformed.reduceByKey {
      (s1: String, s2: String) =>
        s1 + "," + s2
    }
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    reduced.foreach(println)
  }

}
