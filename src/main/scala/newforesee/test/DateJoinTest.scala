package newforesee.test

import org.apache.spark.sql.DataFrame

object DateJoinTest extends Test{
  override def run(): Unit = {
    import spark.implicits._
    val date_df1: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\date.json")
      .withColumn("date",$"date".cast("date"))
      .withColumn("date2",$"date2".cast("date"))
    val date_df2: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\datehavenull.json")
      .withColumn("from_dt",$"from_dt".cast("date"))
      .withColumn("end_dt",$"end_dt".cast("date"))

    date_df1.show()
    date_df2.show()

    val joined_df: DataFrame = date_df2
      .join(date_df1,
        date_df2.col("from_dt")<=date_df1.col("date2")
          and date_df2.col("end_dt")>= date_df1.col("date"),
        "left"
      )

    joined_df.show(50)

  }
}
