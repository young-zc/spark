package day14


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * create by newforesee 2018/10/10
  * 每天每个搜索词用户访问量
  * 1.针对原始数据获得输入RDD
  * 2.使用filter算子,去针对RDD输入的数据进行数据过滤,过滤出符合条件的数据
  * 3.将数据转换"日期_搜索词,用户"格式,对其分组,对每天每个搜索词用户进行去重
  * 4.并统计去重后的数据,即每天每个搜索词的uv
  * 5.最后获取的格式"日期_搜索词,UV"
  * 6.利用DF 取top3
  */
object DaliTop3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DaliTop3").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    //导入隐式转换
    import ssc.implicits._
    // TODO: 前两个需求 下面的代码实现
    //需要的数据,也就是查询条件
    //一般在实际情况中通过MySQL关系数据库查询
    val maps = Map(
      "city" -> List("beijing"),
      "platform" -> List("android"),
      "version" -> List("1.0", "1.2")
    )
    //将查询条件封装为一个Broadcast变量
    //根据我们实现思路分析广播出去是最好的方式,这样可以进行优化每个worker节点就有一份数据了
    val broadcastMap: Broadcast[Map[String, List[String]]] = sc.broadcast(maps)
    //读取数据
    val logRDD: RDD[String] = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/searchLog.txt")
    //通过广播变量筛选符合条件的数据
    val filterLog: RDD[String] = logRDD.filter(log => {
      //与广播变量里的查询条件进行对比,只要该条件设置了,且日志中的数据没有满足条件那么直接false过滤该日志
      //否则返回true,保留该日志信息
      val broadcastValue: Map[String, List[String]] = broadcastMap.value
      //通过get方法获取map中的list值
      val city: List[String] = broadcastValue("city")
      val platform: List[String] = broadcastValue("platform")
      val version: List[String] = broadcastValue("version")
      //切分原始日志,获取城市,平台,版本
      val lines: Array[String] = log.split(",")
      val citys: String = lines(3)
      val platforms: String = lines(4)
      val versions: String = lines(5)
      var flag = true
      //过滤数据 false不保留 true保留
      if (!city.contains(citys)) {
        flag = false
      }
      if (!platform.contains(platforms)) {
        flag = false
      }
      if (!version.contains(versions)) {
        flag = false
      }
      flag
    })
    //filterLog.foreach(println)
    // TODO: 接下来实现第三个需求
    //将过滤出来的日志映射成日期_搜索词,用户
    val dateWordRDD: RDD[(String, String)] = filterLog.map(t => {
      (t.split(",")(0) + "_" + t.split(",")(2), t.split(",")(1))
    })
    //dateWordRDD.foreach(println)
    //进行分组获取每天每个搜索词有哪些用户搜索了(没有去重)
    val dateWordGroupedRDD: RDD[(String, Iterable[String])] = dateWordRDD.groupByKey()
    /*
    (2017-03-13_cloth,CompactBuffer(leo2))
    (2017-03-13_cup,CompactBuffer(leo, leo1, leo3, leo4))
    (2017-03-13_barbecue,CompactBuffer(leo, leo, leo))
    */
    //接下来对每天每个搜索词的搜索用户去重获得UV
    //返回的格式 "日期_搜索词,UV"
    //如果我们用distinct进行去重,将会发生shuffle操作,为了提高性能使用map操作
    val dateWordUVRDD: RDD[(String, Int)] = dateWordGroupedRDD.map(t => {
      //获取到时间和搜索词
      val dateWord: String = t._1
      //获取分组后的所有用户
      val users: Iterator[String] = t._2.iterator
      //创建一个集合进行去重
      val list = new ListBuffer[String]
      while (users.hasNext) {
        val user: String = users.next()
        if (!list.contains(user)) {
          list.append(user)
        }
      }
      //获取到uv访问量
      val uv: Int = list.size
      //返回日期_搜索词,UV
      (dateWord, uv)
    })
    //将每天每个搜索词的UV数据转换成DF

    val dateWordRowRDD: RDD[Row] = dateWordUVRDD.map(row => {
      Row(row._1.split("_")(0), row._1.split("_")(1), row._2.toLong)
    })
    //创建StructType
    val structType = StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("keyword", StringType, nullable = true),
      StructField("uv", LongType, nullable = true)
    ))
    //创建df
    val df: DataFrame = ssc.createDataFrame(dateWordRowRDD, structType)
    //创建临时表
    df.createTempView("date_uv")
    //利用Spark SQL窗口函数,统计每天搜索UV排名前三
    /**
      * select * from
      * (select
      * date,
      * keyword,
      * uv,
      * row_number() over(partition by date order by uv desc) rank
      * from date_uv
      * )
      * where rank<=3
      */
    val sql: DataFrame = ssc.sql("select * from " +
      "(select " +
      "date," +
      "keyword," +
      "uv," +
      "row_number() over(partition by date order by uv desc) rank from date_uv) " +
      "where rank<=3"
    )
    sql.show()


    sc.stop()
  }
}
