package newforesee.test

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import newforesee.Utils.Util
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import vegas.spec.Spec

import scala.util.Properties


/**
  * xxx
  * creat by newforesee 2019-08-08
  */
object Kouhong {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Util.getSpark(this.getClass)
    import spark.implicits._
    val df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/\uD83D\uDC84.json")
    //    df.flatMap((x: Row) =>{
    //      x.getValuesMap(Seq("brands"))
    //    })
    val df1: DataFrame = df
      .withColumn("element", explode($"brands"))
      .withColumn("name", $"element.name")
      .withColumn("series_name", explode($"element.series.name"))
      .withColumn("element", explode($"element.series"))
      .withColumn("lipsticks", explode($"element.lipsticks"))
      .withColumn("color_id", $"lipsticks.id")
      .withColumn("color_code", $"lipsticks.color")
      .withColumn("lipsticks_name", $"lipsticks.name")
      .select("name", "series_name", "color_id", "color_code", "lipsticks_name")
    val properties = new Properties
    properties.load(this.getClass.getClassLoader.getResourceAsStream("db.properties"))
    df1.write.mode("overwrite").jdbc("jdbc:mysql://slave1:3306/gp1809","lipsticks",properties)

    df1.show(1000)

//    df1.rdd.foreachPartition((itr: Iterator[Row]) => {
//      itr.foreach((x: Row) => {
//        println(x.get(0), x.get(1), x.get(2), x.get(3), x.get(4))
//      })
//    })

  //todo:packaging function

//    val sql = "INSERT INTO WORDCOUNT(update_Time,word,count) VALUES(?,?,?)"
//    val connection: Connection = Util.getConnection()
//    val pstmt: PreparedStatement = connection.prepareStatement(sql)
//    Util.getConnection(connection,pstmt,(df1: DataFrame) =>{
//
//          df1.rdd.foreachPartition((ite: Iterator[Row]) =>{
//            ite.foreach((rdd: Row) =>{
//              pstmt.setString(1,rdd.get(0).toString)
//              pstmt.setString(2,rdd.get(1).toString)
//              pstmt.setString(3,rdd.get(2).toString)
//              pstmt.setString(4,rdd.get(3).toString)
//              pstmt.setString(5,rdd.get(4).toString)
//            })
//          })
//        })


  }
}
