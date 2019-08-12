package day11

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/9/30
  */
object JDBCTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JDBC").setMaster("local")
    val sc = new SparkContext(conf)
    val files: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt")
    val words: RDD[String] = files.flatMap((_: String).split(" "))
    val tuples: RDD[(String, Int)] = words.map((_: String,1))
    val result: RDD[(String, Int)] = tuples.reduceByKey((_: Int)+(_: Int))
    //调用foreachPartition这个算子可以提高性能因为可以直接将一个分区所有的数据写入到JDBC中
    //如果不用foreachPartition算子用foreach,那么调用JDBC的次数将和数据的次数差不多所以性能低下
    result.foreachPartition((f: Iterator[(String, Int)]) =>getConnection(f))



  }
  def getConnection(ite:Iterator[(String,Int)]): Unit ={
    val connection: Connection = DriverManager.getConnection(
      "jdbc:mysql://slave1:3306/gp1809?characterEncoding=utf-8",
      "root",
      "123456"
    )
    val pstmt: PreparedStatement = connection.prepareStatement(
      "INSERT INTO WORDCOUNT(update_Time,word,count) VALUES(?,?,?)"
    )
    //写入数据

    try {
      ite.foreach(rdd => {
        pstmt.setDate(1, new Date(System.currentTimeMillis()))
        pstmt.setString(2, rdd._1)
        pstmt.setInt(3, rdd._2)
        val count = pstmt.executeUpdate()
        if (count > 0) {
          println("成功")
        } else {
          println("失败")
        }

      })
    } catch {
      case e:SQLException =>println(e.getMessage)
    }finally {

      pstmt.close()
      connection.close()
    }

  }

}
