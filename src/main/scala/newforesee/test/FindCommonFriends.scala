package newforesee.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark共同好友查找
  * creat by newforesee 2019-07-12
  */
class FindCommonFriends {

}

object FindCommonFriends {
  def main(args: Array[String]): Unit = {
    //输入路径
    val inputPath: String = "/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/test/data"

    //测试环境使用1个内核处理即可，生产环境中进行修改
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("FindCommonFriends")

    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

    val records: RDD[String] = sc.textFile(inputPath)
    //映射两两组合键值对
    val pairs: RDD[((String, String), Seq[String])] = records.flatMap((s: String) => {
      val tokens: Array[String] = s.split(",")
      val person: String = tokens(0)
      val friends: Seq[String] = tokens(1).split("\\s+").toList
      val result: Seq[((String, String), Seq[String])] = for {
        i <- friends.indices
        friend = friends(i)
      } yield {
        if (person < friend)
          ((person, friend), friends)
        else
          ((friend, person), friends)
      }
      result
    })

    //共同好友计算
    val commonFriends: RDD[((String, String), Iterable[String])] = pairs
      .groupByKey()
      .mapValues((iter: Iterable[Seq[String]]) => {
        val friendCount: Iterable[(String, Int)] = for {
          list <- iter
          if list.nonEmpty
          friend <- list
        } yield (friend, 1)
        friendCount.groupBy((_: (String, Int))._1).mapValues((_: Iterable[(String, Int)]).unzip._2.sum).filter(_._2 > 1).map(_._1)
      })

    //保存结果
    //commonFriends.saveAsTextFile(outputPath)

    //打印共同好友结果
    val formatedResult: RDD[String] = commonFriends.sortBy((_: ((String, String), Iterable[String]))._1._1).map(
      (f: ((String, String), Iterable[String])) => s"(${f._1._1}, ${f._1._2})\t${f._2.mkString("[", ", ", "]")}"
    )

    formatedResult.foreach(println)
    sc.stop()
  }
}

object FindCommonFriends2 {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("shareFriend").master("local[1]").getOrCreate()

    ss.sparkContext
      .textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/test/data2")
      .map((_: String).split(":"))
      .flatMap {
        strs: Array[String] =>
          val user: String = strs(0)
          val friends: Array[String] = strs(1).split(",")
          var map: Map[String, (String, String)] = Map[String, (String, String)]()
          friends.foreach((friend: String) => map += (friend -> Tuple2(friend, user)))
          map
      }
      .reduceByKey {
        (t1: (String, String), t2: (String, String)) =>
          (t1._1, t1._2 + "," + t2._2)
      }
      .map {
        x: (String, (String, String)) =>
          val users: Array[String] = x._2._2.split(",")
          x._1 -> users
      }
      .flatMap {
        xx: (String, Array[String]) =>
          val users: Array[String] = xx._2
          val a: Int = users.length
          var b = 0
          var map: Map[String, String] = Map[String, String]()
          while (b < a - 1) {
            var c: Int = b + 1
            while (c < a) {
              map += (users(b) + "+" + users(c) -> xx._1)
              c += 1
            }
            b += 1
          }
          map
      }
      .reduceByKey {
        (f1: String, f2: String) =>
          f1 + "," + f2
      }
      .sortBy((_: (String, String))._1)
      .foreach {
        result: (String, String) =>
          println(result._1 + "\t" + result._2)
      }
  }
}
