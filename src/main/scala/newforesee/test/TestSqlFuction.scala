package newforesee.test

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object TestSqlFuction extends Test{

  import spark.implicits._
  override def run(): Unit = {

//    testReplace
    val dfa: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\a.json")
      .select("name","age","gender")
      .withColumnRenamed("name","namea")
      .withColumnRenamed("age","agea")
      .withColumn("time",current_timestamp())
    dfa.show()

    /*val dfb: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\c.json")
    dfb.show()
    val dfc: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\testsqlfunction.json")
    dfc.show()
    val joinSeq = Seq("name")
    val joined_df: DataFrame = dfa
      .join(dfb,joinSeq,"left")
      .join(dfc,joinSeq,"left")
    joined_df.show()
    val renamed_df: DataFrame = joined_df
      .withColumnRenamed(dfa.col("age").toString(),"agea")
      .withColumnRenamed(dfb.col("age").toString(),"ageb")
    renamed_df.show()*/

  }

  private def testReplace = {
    val df1: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\testsqlfunction.json")
    df1.show()
    val df2: DataFrame = df1
      .withColumn("from", regexp_replace(substring_index(df1.col("from_end"), "-", 1), "：", ":"))
      .withColumn("end", regexp_replace(substring_index(df1.col("from_end"), "-", -1), "：", ":"))
    df2.show()
  }

}
