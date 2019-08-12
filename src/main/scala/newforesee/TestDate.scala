package newforesee

import newforesee.Utils.Util
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
  * xxx
  * creat by newforesee 2019-08-05
  */
object TestDate {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Util.getSpark()
    import spark.implicits._
    //spark.read.schema(schema).json(file).cache()
    val df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/date.json")
    df.show()
//    val df3: DataFrame = df
//      .withColumn("date", date_format($"date", "yyyy-MM"))
//      .withColumn("date2", date_format($"date2", "yyyy-MM"))
//    df3.show()
    val df4: DataFrame = df.withColumn("MAB",months_between(date_format($"date2", "yyyy-MM"),date_format($"date", "yyyy-MM")))
    df4.show()



  }

}
