package youngtest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TestCollect {

  def main(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession.builder().appName("shareFriend").master("local").getOrCreate()
    val sc: SparkContext = ss.sparkContext
    val srcData: DataFrame = ss.read.textFile("F:\\git_library\\spark_examples\\src\\students.txt").toDF()
    val src: Array[Row] = srcData.collect()
}
}
