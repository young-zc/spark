package day13


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

/**
  * spark Sql 内置函数统计每天销售额
  */
object DFandMoney {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DFandMoney").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val Dframe: DataFrame = ssc.read.format("json").load("/Users/newforesee/Intellij Project/Spark/src/usermoney.json")
    val rowRDD: RDD[Row] = Dframe.rdd.map(t => {
      Row(t.getAs[String](0), t.getAs[Double](1))
    })
    //创建structType
    val structType = StructType(Array(StructField("date",StringType,true),StructField("money",DoubleType,true)))
    val df: DataFrame = ssc.createDataFrame(rowRDD,structType)
    //导入隐式转换
    import ssc.implicits._
    import org.apache.spark.sql.functions._
    //根据时间聚合money
    df.groupBy("date").agg('date,sum("money")).rdd.map(row=>Row(row(1),row(2))).collect().foreach(println)
  }
}
