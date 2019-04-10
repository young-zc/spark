package day12


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 第二种 使用编程方式构建DF
  */
object RDD2DataFrameScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrameScala").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val files: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/students.txt")
    val rowRDD: RDD[Row] = files.map(t => {
      val splits: Array[String] = t.split(",")
      Row(splits(0).toInt, splits(1), splits(2).toInt)
    })
    //构建structType
    val structType: StructType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name",StringType, true),
      StructField("age", IntegerType, true)))
    val df = ssc.createDataFrame(rowRDD,structType)
    df.registerTempTable("stu")
    val sql: DataFrame = ssc.sql("select * from stu where age > 17")
    sql.show()
    val rdd: RDD[Row] = sql.rdd
    rdd.foreach(println)

  }
}
