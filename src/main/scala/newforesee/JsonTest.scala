package newforesee

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Json").master("local").getOrCreate()
    import spark.implicits._
    val tel: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/telimatic.json")

    tel.show


  }

}
