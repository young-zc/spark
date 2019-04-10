package newforesee

import java.sql.{Connection, ResultSet, Statement}

import day18.ConnectionPool
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
  * creat by newforesee 2018/11/16
  */
object MySql2HDFS {
  def main(args: Array[String]): Unit = {
    //    val reader = spark.sqlContext.read.format("jdbc")
    //        reader.option("url", url)
    //        reader.option("dbtable", table)
    //        reader.option("driver", "com.mysql.jdbc.Driver")
    //        reader.option("user", "root")
    //        reader.option("password", password)


    val session: SparkSession = SparkSession.builder().appName("mysql2hdfs").master("local").getOrCreate()
    val reader: DataFrameReader = session.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://slave1:3306/gp1809")
      .option("dbtable", "Persons")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password", "123456")
    val df: DataFrame = reader.load()
    df.show()

//    val conn: Connection = ConnectionPool.getConnection
//    val sql: String = "select * from Persons"
//    val statement: Statement = conn.createStatement()
    //    session.sparkContext.parallelize(statement.executeQuery(sql))
    //    statement.executeQuery(sql)
  }
}
