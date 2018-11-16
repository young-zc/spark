package day14

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 强类型
  */
object Type {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("Type").master("local").getOrCreate()
    import session.implicits._
    val df: DataFrame = session.read.json("/Users/newforesee/Intellij Project/Spark/src/a.json")
    //将df转化为ds
    val ds: Dataset[PROs] = df.as[PROs]
    //println(ds.rdd.partitions.size)
    //改变分区可以用repartition和coalesce
    val repartitions: Dataset[PROs] = ds.repartition(5)
    //现在是dataset类型需要转换RDD
    //println(repartitions.rdd.getNumPartitions)
    //改变分区,用coalesce
    val coalesces: Dataset[PROs] = repartitions.coalesce(2)
    //println(coalesces.rdd.getNumPartitions)
    //去重操作 distinct   dropDuplicates
    //区别在于  distinct是根据每一条数据进行完整内容的比对去重
    //而dropDuplicates可以根据指定的字段去重
    val dis: Dataset[PROs] = coalesces.distinct()
    val drop: Dataset[PROs] = coalesces.dropDuplicates("name")
    //dis.show()
//      +---+-----+------+------+------+
//      |age|depId|gender|  name|salary|
//      +---+-----+------+------+------+
//      | 30|    2|female|   Jen| 28000|
//      | 35|    1|  male|  Jack| 15000|
//      | 21|    3|female|Kattie| 21000|
//      | 30|    2|female| Marry| 25000|
//      | 19|    2|female|   Jen|  8000|
//      | 25|    1|  male|   Leo| 20000|
//      | 42|    3|  male|   Tom| 18000|
//      +---+-----+------+------+------+
    /*drop.show()*/
//      +---+-----+------+------+------+
//      |age|depId|gender|  name|salary|
//      +---+-----+------+------+------+
//      | 35|    1|  male|  Jack| 15000|
//      | 42|    3|  male|   Tom| 18000|
//      | 30|    2|female|   Jen| 28000|
//      | 30|    2|female| Marry| 25000|
//      | 21|    3|female|Kattie| 21000|
//      | 25|    1|  male|   Leo| 20000|
//      +---+-----+------+------+------+

    val df1: DataFrame = session.read.json("/Users/newforesee/Intellij Project/Spark/src/c.json")
    val ds1: Dataset[PROs] = df1.as[PROs]
    /**

    //获取当前Dataset中有的另一个Dataset中没有的数据
    ds.except(ds1).show()
    //过滤
    ds.filter("age>30").show()
    //求交集
    ds.intersect(ds1).show()
    ds.map(t=>(t.name,t.age)).show()

    */
    ds.sort($"age".desc,$"salary".desc).show()
//      +---+-----+------+------+------+
//      |age|depId|gender|  name|salary|
//      +---+-----+------+------+------+
//      | 42|    3|  male|   Tom| 18000|
//      | 35|    1|  male|  Jack| 15000|
//      | 30|    2|female|   Jen| 28000|
//      | 30|    2|female| Marry| 25000|
//      | 25|    1|  male|   Leo| 20000|
//      | 21|    3|female|Kattie| 21000|
//      | 19|    2|female|   Jen|  8000|
//      +---+-----+------+------+------+

    session.stop()

  }
}
case class PROs(name:String,age:Long,depId:Long,gender:String,salary:Long)