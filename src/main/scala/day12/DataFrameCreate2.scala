package day12

import java.sql.{Connection, Date, DriverManager, PreparedStatement, SQLException}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by newforesee 2018/10/8
  */
object DataFrameCreate2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DFC").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    //读取JSON数据
    //相当于创建一张表
    val df = ssc.read.json("/Users/newforesee/Intellij Project/Spark/src/main/students.json")
    //查看文件内容，默认只显示前20条，相当于select * from 表名
    //df.show()
    //显示多少条数据
    //df.show(2)
    //内容是否显示最多20个字符，默认为true
    //df.show(false)
    //综合设置
    //df.show(3，false)
    //打印元数据信息
    //    df.printSchema()
    //和show方法不同，这个方法表示将所有数据获取，然后返回一个Array对象
    //df.collect()
    //传入字段类型名字，统计数值类型字段的值比如count，平均值最大值最小值差值
    //df.describe("age","name").show()
    //获取第一行数据
    //println(df.first())
    //df.where("id = 1 or name = 'leo'")
    //过滤 与where类似
    //df.filter("id = 1").show()
    //查询指定字段
    //df.select(“id”，“name”).show
    //获取指定字段 主要是结合使用
    //println(df.col("id"))
    //去除指定字段
    //df.drop("id")
    //df.orderBy(df.col("age")).show()
    //倒序
    //df.sort(-df.col("age")).show()
    //分组
    //df.groupBy("id")
    //去重
    //df.distinct().show()
    //按照指定字段去重,里面可以传入Array和Seq类型
    //df.dropDuplicates(Seq("id")).show()
    //查询某几列所有数据,并对列进行计算
    //df.select(df.col("name"),df.col("age").plus(1)).show()
    //根据某一列的值进行过滤
    //df.filter(df.col("age").gt("18")).show()
    df.groupBy(df.col("age")).count().show()

  }
}
