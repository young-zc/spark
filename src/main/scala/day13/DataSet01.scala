package day13

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 案例分析:计算部门平均薪资和年龄
  * 1.只统计年龄在20岁以上的员工
  * 2.根据部门名称和员工性别为要求进行统计
  * 3.统计出每个部门性别的平均薪资和平均年龄
  */
object DataSet01 {
  def main(args: Array[String]): Unit = {
    //首先第一步在spark2.0版本以后可以直接使用sparkSession不需要conf和context
    val spark: SparkSession = SparkSession.builder().appName("dateset1").master("local").getOrCreate()
    //导入隐式转换和spark SQL中的内置函数
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //首先将两个文件加载进来形成两个DataFrame
    val df1: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    val df2: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/b.json")
    //根据需求进行处理过滤20以下员工
    //第二个需求根据部门名称和员工进行统计,那么就将两个表join
    //join 的第一个参数是表名,第二个 参数是连接条件
    df1.filter("age > 20").join(df2,$"depID"===$"id")
    //根据join后的数据进行分组
//    +---+-----+------+------+------+---+--------------------+
//    |age|depId|gender|  name|salary| id|                name|
//    +---+-----+------+------+------+---+--------------------+
//    | 25|    1|  male|   Leo| 20000|  1|Technical Department|
//    | 30|    2|female| Marry| 25000|  2|Financial Department|
//    | 35|    1|  male|  Jack| 15000|  1|Technical Department|
//    | 42|    3|  male|   Tom| 18000|  3|       HR Department|
//    | 21|    3|female|Kattie| 21000|  3|       HR Department|
//    | 30|    2|female|   Jen| 28000|  2|Financial Department|
//    +---+-----+------+------+------+---+--------------------+
      .groupBy(df2("name"),df1("gender"))
    //然后进行聚合操作
      .agg(avg(df1("salary")),avg(df1("age")))
    //执行Action操作,将结果显示
      .show()
  }

}
