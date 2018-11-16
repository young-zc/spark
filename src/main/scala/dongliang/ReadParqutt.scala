package dongliang

import org.apache.spark.sql
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * creat by newforesee 2018/11/10
  */
object ReadParqutt {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder.appName("ss").master("local").getOrCreate()
    val df: DataFrame = session.read.parquet("/Users/newforesee/Intellij Project/Spark/src/main/scala/dongliang/20180715.parquet")
    df.printSchema()
    df.show(5)

  }

}
