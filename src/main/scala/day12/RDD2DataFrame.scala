package day12

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 第一种方式 反射的方式创建DF
  */
object RDD2DataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DF").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val files = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/students.txt")
    val student = files.map(t => {
      val lines = t.split(",")
      students(lines(0).toInt, lines(1), lines(2).toInt)
    })
    //需要导入隐式转换
    import ssc.implicits._
    //创建DateFrame
    val df: DataFrame = student.toDF()
    //注册临时表
    df.registerTempTable("student")
    //针对这个临时表执行sql查询
    val dfs: DataFrame = ssc.sql("select * from student where age >= 18")
    //dfs.write.mode(SaveMode.Append).
    dfs.show()
  }
}
case class students(id:Int,name:String,age:Int)
