package newforesee.test

import org.apache.spark.sql.DataFrame

object TestLeftJoin extends Test {
  override def run(): Unit = {
    val dfa: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\a.json")
    val dfc: DataFrame = spark.read.json("D:\\workspace\\IDEA\\spark_examples\\src\\c.json")
    val joined_df: DataFrame = dfa
      .join(dfc,dfa.col("name")===dfc.col("name"),"left")
    joined_df.show()


  }
}
