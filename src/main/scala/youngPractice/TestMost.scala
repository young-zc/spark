package youngPractice

import newforesee.test.Test
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable

object TestMost extends Test{
  override def run(): Unit = {

    import spark.implicits._

    val data_df: DataFrame = spark.read.json("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\userlog.json")
        .withColumn("name",array("name"))
    data_df.show()

    val result_df: DataFrame = data_df.groupBy("date").agg(getMost($"name"))
    result_df.show()
  }


  def getMost: UserDefinedFunction = udf(
    (arr: mutable.WrappedArray[String]) => {
      val reverse: List[(String, Int)] =
        arr.filter((x: String) =>{
          x.nonEmpty && x != null && x!= " "
        } ).map((_: String, 1))
          .groupBy((x: (String, Int)) => x._1)
          .mapValues((_: mutable.WrappedArray[(String, Int)]).size)
          .toList
          .sortBy((_: (String, Int))._2)
          .reverse
      reverse.head._1
    }
  )
}
