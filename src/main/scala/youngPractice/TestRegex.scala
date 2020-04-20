package youngPractice

import newforesee.test.Test
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

object TestRegex extends Test{

  override def run(): Unit = {
    import spark.implicits._
    val df: DataFrame = spark.read.json("src\\main\\scala\\youngPractice\\rlike.json")
    df.show()
    println("-------------------------------")
    //df.filter($"name".rlike("^[a-zA-Z0-9]+\\.?+[a-zA-Z0-9]+$")).show()
    val df1: DataFrame = df
      .withColumn("name", when($"name".rlike("^[a-zA-Z0-9]+\\.?+[a-zA-Z0-9]+$"), $"name").otherwise(null))
    df1.show(30,false)
  }
}
