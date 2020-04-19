package newforesee.test

import newforesee.Utils.Util
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TestLeftJoin extends Test {

  import spark.implicits._
  override def run(): Unit = {
    val dfa: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\a.json")
    dfa.show()
    val dfc: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\c.json")
    dfc.show()
    val joined_df: DataFrame = dfa.join(dfc,dfa.col("depId")===1,"left")
    joined_df.show()
   /* val joined_df: DataFrame = dfa
      .join(dfc,dfa.col("name")===dfc.col("name"),"left")
    joined_df.show()*/
    /*dfc.withColumn("time1",current_timestamp()).show(false)
    dfc.withColumn("time2",unix_timestamp()).show()
    val crosjoin_df: DataFrame = dfa.crossJoin(dfc)
      .withColumn("time",Util.datetimeUTC2CST(current_timestamp()))
    crosjoin_df.show(false)*/
//    val leftjoin_df: DataFrame = dfa.join(dfc,dfc.col("depId")===1,"left")
//    leftjoin_df.show()

  }
}
