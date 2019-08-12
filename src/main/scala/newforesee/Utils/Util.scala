package newforesee.Utils

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import org.apache.spark.sql.{DataFrame, SparkSession}
import vegas.spec.Spec.DataFormat

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
