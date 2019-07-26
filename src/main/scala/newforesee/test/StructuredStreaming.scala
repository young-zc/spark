package newforesee.test

import newforesee.Utils.Util
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql._
import functions._

/**
  * xxx
  * creat by newforesee 2019-07-17
  */
object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = Util.getSpark(this)
    import spark.implicits._
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
    val words: DataFrame = lines.as[String].flatMap((_: String).split(" "))
      .withColumn("a", lit(1))
      .groupBy("value").count()

    //val www: DataFrame = words.groupBy("value").pivot("a").count()

    val query: StreamingQuery = words.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }

}
