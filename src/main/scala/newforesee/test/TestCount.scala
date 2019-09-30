package newforesee.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object TestCount extends Test{

  import spark.implicits._
  override def run(): Unit = {
    val data_df: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\count.json")
      .withColumn("sta",$"sta".cast("boolean"))
      .withColumn("new_name",$"name".as("new_name"))
    data_df.show()
    /*val df2: DataFrame = data_df.groupBy("name", "age")
      .agg(count(lit(1)).as("count"),
        sum("salary").as("sum")
      )
    df2.show()*/
    data_df.printSchema()


    val result_df: DataFrame = data_df
      .withColumn("testdate",current_date().cast("timestamp"))
      .withColumn("timestampdate",to_timestamp(current_date()))
      .withColumn("unix",current_timestamp())
      .withColumn("sta",$"sta".cast("int"))

    result_df.show()
  }
}
