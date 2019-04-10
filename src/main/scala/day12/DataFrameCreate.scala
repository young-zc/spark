package day12

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/10/8
  */
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DFC").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val df: DataFrame = ssc.read.text("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt")
    df.show()



  }


}
