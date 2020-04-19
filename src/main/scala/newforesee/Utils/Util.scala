package newforesee.Utils

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * xxx
  * creat by newforesee 2019-07-17
  */
object Util {
  def getSpark(clazz: Any): SparkSession = {

    SparkSession.builder().master("local").appName(clazz.getClass.getSimpleName).getOrCreate()
  }

  def getConnection() = {
    val connection: Connection = DriverManager.getConnection(
      "jdbc:mysql://slave1:3306/gp1809?characterEncoding=utf-8",
      "root",
      "123456"
    )
    connection
  }

  def datetimeUTC2CST = udf {
    (utcStr: String) => {

      if (utcStr == null) {
        "1900-01-01 00:00:00"
      } else if (utcStr.isEmpty) {
        "1900-01-01 00:00:00"
      } else {
        val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS")
        //val formater2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        formater.format(formater.parse(utcStr).getTime + 28800000)
      }
    }
  }

  def getConnection(connection: Connection, pstmt: PreparedStatement, func: DataFrame => Unit) = {
    //    val pstmt: PreparedStatement = connection.prepareStatement(
    //      sql
    //      // "INSERT INTO WORDCOUNT(update_Time,word,count) VALUES(?,?,?)"
    //    )
    //写入数据

    try {
      //      ite.foreach((rdd: (String, Int)) => {
      //        pstmt.setDate(1, new Date(System.currentTimeMillis()))
      //        pstmt.setString(2, rdd._1)
      //        pstmt.setInt(3, rdd._2)
      //        val count = pstmt.executeUpdate()
      //        if (count > 0) {
      //          println("成功")
      //        } else {
      //          println("失败")
      //        }
      //
      //      })
      func
      val count = pstmt.executeUpdate()
      if (count > 0) {
        println("成功")
      } else {
        println("失败")
      }

    } catch {
      case e: SQLException => println(e.getMessage)
    } finally {

      pstmt.close()
      connection.close()
    }

  }

}
