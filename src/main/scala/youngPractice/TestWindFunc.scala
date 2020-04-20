package youngPractice

import newforesee.test.Test
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object TestWindFunc extends Test{

  import spark.implicits._
  override def run(): Unit = {
    spark.conf.set("spark.sql.crossJoin.enabled",true)
    //getDiffDateData

    val srcDF: DataFrame = spark.read.json("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\testover_windowdate.json")
    srcDF.show()
    val wind: WindowSpec = Window.partitionBy("id").orderBy("edate")
    val sortDF: DataFrame = srcDF.withColumn("rank",row_number().over(wind))
    sortDF.show()
    sortDF.select("id","edate","bdate").where($"rank"===1).show()

  }

  private def getDiffDateData: Unit = {
    val df: DataFrame = spark.read.json("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\testover_windowdate.json")
    val dfdd: DataFrame = spark.read.json("C:\\Users\\RG316\\IdeaProjects\\spark_example\\src\\testover_windowdate.json")
    /*val wind: WindowSpec = Window.partitionBy("id").orderBy("id")
    val df1: DataFrame = df.select($"id",$"bdate",$"edate",row_number().over(wind))
    df1.show()
    val w: WindowSpec = Window.partitionBy("id")
    val df2: DataFrame = df1.where(($"edate"-$"bdate").over(w)<10)
    df2.show()*/

    df.join(df, df("id") === df("id") && df("edate") - df("bdate") >= 0 && df("edate") - df("bdate") <= 10).show()
    df.join(dfdd, df("id") === dfdd("id") && df("edate") - dfdd("bdate") >= 0 && df("edate") - dfdd("bdate") <= 10).show()
    val df1: DataFrame = df.select("id", "bdate").withColumnRenamed("bdate", "date")
    df.join(df1, df("id") === df1("id") && df("edate") - df1("date") >= 0 && df("edate") - df1("date") <= 10).show()
  }
}
