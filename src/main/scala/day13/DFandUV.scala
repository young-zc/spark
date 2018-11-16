package day13

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark SQl内置函数操作
  */
object DFandUV {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DFandUV").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val dataFrame: DataFrame = ssc.read.json("/Users/newforesee/Intellij Project/Spark/src/userlog.json")
    //构建RDD[row]
//    val rowRDD = dataFrame.map(t => {
//      Row(t.get(0), t.getInt(1))
//    })
    val rowRDD: RDD[Row] = dataFrame.rdd.map(t => {
      Row(t.get(0), t.get(1).toString.toInt)
    })


    //接下来创建stuctType
    val structType: StructType = StructType(Array(StructField("date",StringType,true),StructField("userid",IntegerType,true)))
    val df = ssc.createDataFrame(rowRDD,structType)
    //接下来使用spark SQL的内置海曙进行数据处理
    //在这里注意一下,我们使用内置函数需要导入隐式转换
    import ssc.implicits._
    //首先我们先进行数据的分组,然后进行聚合去重
    //还需要导入spark SQL内置函数的包
    import org.apache.spark.sql.functions._
    df.groupBy("date").agg('date,countDistinct("userid")).rdd
      .map(row=>Row(row(1),row(2))).collect.foreach(println)
  }
}
