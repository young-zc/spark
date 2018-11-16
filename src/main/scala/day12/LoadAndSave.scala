package day12

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据源的读取和存储
  */
object LoadAndSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LoadAndSave").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    //load默认读取文件格式是parquet文件,但是我们可以通过.forma()进行更改读取方式.
    //这里注意如果我们读取parquet文件的话那么此文件的内容是不能打印出来的可以进行保存
    //val df: DataFrame = ssc.read.load("/Users/newforesee/Intellij Project/Spark/src/u.parquet")
    //df.show()
    //ssc.read.format("json").load("")
    val df: DataFrame = ssc.read.json("/Users/newforesee/Intellij Project/Spark/src/userlog.json")
    //保存数据
    //df.write.save("day12.txt")
    //    df.write.jdbc()
    //    df.write.format("json").mode(SaveMode.Append).save()
  }
}
