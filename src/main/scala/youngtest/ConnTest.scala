package youngtest

import java.util.Properties

import newforesee.test.Test
import org.apache.spark.sql.DataFrame

object ConnTest extends Test{
  override def run(): Unit = {

    val properties = new Properties
    properties.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    properties.setProperty("url","jdbc:sqlserver://sql01cnprd19907.database.chinacloudapi.cn:1433;database=sqldb01cnprd19907")
    properties.setProperty("user","Cmabi")
    properties.setProperty("password","Cci12345")

    val src_table: DataFrame = spark.read.jdbc(properties.getProperty("url"),"User_T",properties)
    src_table.show()
  }
}
