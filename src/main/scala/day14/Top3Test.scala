package day14

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * creat by newforesee 2018/10/13
  * 每天每个搜索词用户访问量
  *  1.针对原始数据获得输入RDD
  *  2.使用filter算子,去针对RDD输入的数据进行数据过滤,过滤出符合条件的数据
  *  3.将数据转换"日期_搜索词,用户"格式,对其分组,对每天每个搜索词用户进行去重
  *  4.并统计去重后的数据,即每天每个搜索词的uv
  *  5.最后获取的格式"日期_搜索词,UV"
  *  6.利用DF 取top3
  */
object Top3Test extends App {
  // 2017-03-13,leo4,cup,beijing,android,1.0
  private val session: SparkSession = SparkSession.builder().appName("Top3test").master("local").getOrCreate()
  private val frame: DataFrame = session.read.csv("/Users/newforesee/Intellij Project/Spark/src/searchLog.txt")
  private val mapFilter = Map(
    "city" -> List("beijing"),
    "platform" -> List("android"),
    "version" -> List("1.0","1.2")
  )
  private val broadcastmap: Broadcast[Map[String, List[String]]] = session.sparkContext.broadcast(mapFilter)

  private val filtered: Dataset[Row] = frame.filter(row => {
    var flag = true
    val broadcastValue: Map[String, List[String]] = broadcastmap.value
    val citys: List[String] = broadcastValue("city")
    val platforms: List[String] = broadcastValue("platform")
    val versions: List[String] = broadcastValue("version")
    if (!citys.contains(row(3)) || !platforms.contains(row(4)) || !versions.contains(row(5))) {
      flag = false
    }
    flag
  })

  private val formated: RDD[(String, Any)] = filtered.rdd.map((row: Row) => {
    (row(0) + "_" + row(2), row(1))
  })
  private val rowRDD: RDD[Row] = formated.map(row=>Row(row._1,row._2))
  private val structType = StructType(Array(StructField("dateandword",StringType,true),StructField("uv",IntegerType,true)))
  private val df: DataFrame = session.createDataFrame(rowRDD,structType)
  df.createTempView("table")
  /**
  select * from
        (select
            date,
            keyword,
            uv,
            row_number() over(partition by date order by uv desc) rank
            from date_uv
            )
        where rank<=3
    */
//  session.sql("select * from " +
//    "(select dateandword,uv," +
//    "row_number() over(order by uv) rank from table)" +
//    "where rank <= 3").show()


}

