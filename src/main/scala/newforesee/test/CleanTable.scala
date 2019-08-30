package newforesee.test

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._

/**
  * xxx
  * creat by newforesee 2019-08-13
  */
object CleanTable extends Test {
  override def run(): Unit = {
    import spark.implicits._
    val df1: DataFrame = spark.read.csv("/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/test/accccc.csv")
    val struct: StructType = (new StructType)
      .add("FAILURE_PART", StringType)
      .add("ENGINE_NAME", StringType)
      .add("ROLE", StringType)
    val df2: DataFrame = spark.createDataFrame(df1.rdd,struct)
    logger.warn(s"df2 count >>> %s".format(df2.count()))
    val df3: Dataset[Row] = df2.distinct().filter($"ROLE" isNotNull)
    logger.warn(s"df3 count >>> %s".format(df3.count()))
    println(s" distinct %s rows".format(df2.count()-df3.count()))
    df3.show()
    df3.coalesce(1).write.parquet("acc_table")
//    val df11: DataFrame = spark.read.parquet("/Users/newforesee/Intellij Project/Spark/acc_table")
//    df11.printSchema()
//    df11.show()



  }



  def subStringWithSpec: UserDefinedFunction = udf{
    str:String=>{
      if (str!=null&&str.nonEmpty)
        str.substring(0,str.indexOf("_"))
      else
        str
    }
  }
}
