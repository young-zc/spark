package day14

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * create by newforesee 2018/10/10
  *
  * 弱类型 select where groupBy agg jion...
  */
object UbType {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("untype").master("local").getOrCreate()
    import session.implicits._
    //加载数据
    val df: DataFrame = session.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    val df2: DataFrame = session.read.json("/Users/newforesee/Intellij Project/Spark/src/b.json")
    df.where("age > 20").join(df2,$"depId"===$"id")
      .groupBy(df2("name"),df("gender"))
      .agg(avg("salary"))
      .select($"name",$"gender",$"avg(salary)")
      .show()
  }
}
