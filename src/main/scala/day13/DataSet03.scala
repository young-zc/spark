package day13

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSet03 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DS3").master("local").getOrCreate()
    //导入隐式转换
    import spark.implicits._
    val df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    //创建临时视图,主要是为了可以使用SQL
    //df.createTempView("tmp")
    //可以对视图执行SQL
    //spark.sql("select * from tmp where age > 30").show()
    //获取元数据信息
    //df.printSchema()
    //c存储
    //df.write.json("df")
    //转换DataSet
    val pro: Dataset[Pro] = df.as[Pro]
    pro.show()




    spark.stop()
  }

}
case class Pro(name:String,age:Long,dpId:Long,gender:String)
