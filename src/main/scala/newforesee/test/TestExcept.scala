package newforesee.test

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

/**
  * spark DataFrame 交差并集测试
  * creat by newforesee 2019-08-26
  */
object TestExcept extends Test {

 /* a.json
  {"name": "Leo", "age": 25, "depId": 1, "gender": "male", "salary": 20000}
  {"name": "Marry", "age": 30, "depId": 2, "gender": "female", "salary": 25000}
  {"name": "Jack", "age": 35, "depId": 1, "gender": "male", "salary": 15000}
  {"name": "Tom", "age": 42, "depId": 3, "gender": "male", "salary": 18000}
  {"name": "Kattie", "age": 21, "depId": 3, "gender": "female", "salary": 21000}
  {"name": "Jen", "age": 30, "depId": 2, "gender": "female", "salary": 28000}
  */
  /*c.json
  {"name": "Leo", "age": 25, "depId": 1, "gender": "male", "salary": 20000}
  {"name": "Marry", "age": 30, "depId": 2, "gender": "female", "salary": 25000}
  {"name": "Jack", "age": 35, "depId": 1, "gender": "male", "salary": 15000}
  {"name": "Tom", "age": 42, "depId": 3, "gender": "male", "salary": 18000}
  {"name": "Kattie", "age": 21, "depId": 3, "gender": "female", "salary": 21000}
  {"name": "Jen", "age": 30, "depId": 2, "gender": "female", "salary": 29000}
  {"name": "Jens", "age": 19, "depId": 2, "gender": "female", "salary": 8000}
     */
  override def run(): Unit = {
    //历史全量数据
    val df_a: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    .withColumn("update_date", lit("2019-08-21 21:05:11"))

    //新的全量数据
    val df_c: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/c.json")
    df_a.show()
    df_c.show()

    val witnoutdate: DataFrame = df_a.drop("update_date")
    //    //差集
    val IncrementalData: Dataset[Row] = df_c.exceptAll(witnoutdate)
    val olddata: DataFrame = df_c.join(df_a, df_c.columns, "inner")
    olddata.union(IncrementalData.withColumn("IncrementalData",from_unixtime(unix_timestamp(),"yyyy-MM-dd HH:mm:ss"))).show()
    //    //交集
    //    df_a.intersect(df_c).show()
    //    //并集(不就是union嘛啊喂)
    //    df_a.union(df_c).distinct().show()





  }
}
