package youngtest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GetTogethFriends {

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("shareFriend").master("local").getOrCreate()

    //读取文件，并把文件中原有(user:friend,friend,···)的形式转换为(friend,(friend,user))的形式

    /**
      * A, "B,C,D,F,E,N"
      * B:A,C,E,K
      * C:F,A,D,I
      * D:A,E,F,L
      */
    val flatmap1: RDD[(String, (String, String))] = ss.sparkContext
      .textFile("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\commonfriends")
      .map((_: String).split(":"))
      .flatMap {
        strs: Array[String] =>
          val user: String = strs(0)
          val friends: Array[String] = strs(1).split(",")
          var map: Map[String, (String, String)] = Map[String, (String, String)]()
          friends.foreach((friend: String) => map += (friend -> Tuple2(friend, user)))
          map
      }
    /*
    B->(B,A)
    C->(C,A)
    D->(D,A)
     */

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>flatmap1")
    flatmap1.foreach(println)

    //reducebykey:按照friend把所有的user聚合到一块儿，包含friend本身，例如(friendA,(friendA,userA,userB,···))
    val reducebykey1: RDD[(String, (String, String))] = flatmap1
      .reduceByKey {
        (t1: (String, String), t2: (String, String)) =>
          (t1._1, t1._2 + "," + t2._2)
      }

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>reducebykey1")
    reducebykey1.foreach(println)

    //剔除上一RDD中的friend，数据形如(friend,(userA,userB,···))
    val reduce1_map: RDD[(String, Array[String])] = reducebykey1
      .map {
        x: (String, (String, String)) =>
          val users: Array[String] = x._2._2.split(",")
          x._1 -> users
      }

    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>reduce1_map")
    reduce1_map.foreach(x=>{
      println(x._1 + ":"+
        x._2.mkString("",",","") )
    })

    //根据上一个RDD（数据格式：(friend,(userA,userB,···))）的结果，列出每两个user对应的一个friend的所有可能
    val flatmap2: RDD[(String, String)] = reduce1_map
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

    flatmap2
      //聚合，把每两个user对应的所有共同好友聚合起来
      .reduceByKey {
        (f1: String, f2: String) =>
          f1 + "," + f2
      }
      //排序
      .sortBy((_: (String, String))._1)
      .foreach {
        result: (String, String) =>
          println(result._1 + "\t" + result._2)
      }
  }

}
