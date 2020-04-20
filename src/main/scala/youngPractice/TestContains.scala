package youngPractice

import newforesee.test.Test
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object TestContains extends Test{
  import spark.implicits._
  override def run(): Unit = {

    val df: DataFrame = spark.read.option("delimiter","\t").text("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\teacher.txt")
    //val df1: Dataset[Row] = df.select("value").where($"value".contains(List("en","si"):_*))
    val df2: Dataset[Row] = df.select("value").where($"value".isin(List("en","si"):_*))
    val strList = Seq("en","si","a bi","语","math")
    val rdd: DataFrame = spark.sparkContext.makeRDD(strList).toDF("str")
    df.join(rdd,upper(df("value")).contains(upper(rdd("str")))).show()
    df.show()
    //df2.show()
  }
}
