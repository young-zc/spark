package newforesee.test

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TestCastDate extends Test {

  import spark.implicits._
  override def run(): Unit = {

    val fi_df: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\a.json")
    val se_df: DataFrame = fi_df.withColumn("time",lit("2019-11-06 15:09:40.000").cast("timestamp"))
    se_df.show(false)
    val th_df: DataFrame = se_df.withColumn("time",from_utc_timestamp($"time","GMT+8"))
    th_df.show(false)
  }


  def datetimeUTC2CST = udf {
    (utcStr: String) => {
      if (utcStr == null) {
        "1900-01-01 00:00:00.000"
      } else if (utcStr.isEmpty) {
        "1900-01-01 00:00:00.000"
      } else {
        val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        formater.format(formater.parse(utcStr).getTime + 28800000)
      }
    }
  }
}
