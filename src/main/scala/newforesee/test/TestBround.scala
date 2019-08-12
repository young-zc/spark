package newforesee.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * xxx
  * creat by newforesee 2019-08-12
  */
object TestBround extends Test {

  import spark.implicits._

  override def run(): Unit = {
    val df1: DataFrame = spark.read.csv("/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/test/floats")
    df1
      .withColumn("c1", bround($"_c0", 2))
      .withColumn("c2", bround($"_c0", 0))
      .show()
    //    df1.withColumn("aa",$"_c0")
  }
}
