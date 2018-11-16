package day13

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * spark SQL的窗口函数
  */

object RowNumber {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DFandMoney").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc =new SQLContext(sc)
    val lines: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/teacher.txt")
    val teacher: RDD[teachers] = lines.map(t => {
      val s: Array[String] = t.split(" ")
      teachers(s(0), s(1))
    })
    //导入隐式转换
    import ssc.implicits._
    //这里将字段名字重新赋值构建df
    val df: DataFrame = teacher.toDF("subject","teacher")
    df.registerTempTable("teachers")
    //现在将里面最喜欢的老师聚合,也就是分组聚合
    val df1: DataFrame = ssc.sql("select subject,teacher,count(1) as counts from teachers group by subject,teacher")
    //df1.show()
//      +-------+--------+------+
//      |subject| teacher|counts|
//      +-------+--------+------+
//      | javaee|zhangsan|    10|
//      | javaee|   laoqi|     6|
//      |bigdata| laozhao|     3|
//      |    php| liuneng|     7|
//      |    php|guangkun|     8|
//      | javaee| laowang|     4|
//      |bigdata|   laosi|     5|
//      +-------+--------+------+
    //接下来开始使用开窗函数,进行数据统计,开窗函数相当于一个伪列
    //我们不可以直接使用但是可以当成一个结果集然后调用
    //如果我们想用之前获取的结果集,name需要将之前的结果集注册为临时表
    df1.registerTempTable("teacher_tmp")
    //这里我们要调用到row_number() over()进行排序,倒序显示
    val df2: DataFrame = ssc.sql("select * from " +
      "(select *,row_number() over(partition by subject order by counts desc ) rank from teacher_tmp)" +
      " t where rank < 2 ")
    df2.show()
  }
}
case class teachers(x:String,y:String)
