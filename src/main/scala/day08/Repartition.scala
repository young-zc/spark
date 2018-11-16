package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Repartition").setMaster("local")
    val sc = new SparkContext(conf)
    //repartition算子， 用于任意将RDD的partition增多，或者减少
    //与coalesce不同之处在于，coalesce默认是将分区减少
    //但是我们调用Repartition算子是将分区增加

    //公司要增加新的部门

    val list = List("张三","李四","王五","赵六","赵钱孙","吴正旺","冯陈楚","蒋沈晗","朱亲友","徐博文")
    val nameRDDs: RDD[String] = sc.parallelize(list,3)
    //原来的分配
    val nameRDD2: RDD[(Int, String)] = nameRDDs.mapPartitionsWithIndex(mapPartitionsIndex3,true)
        nameRDD2.collect.foreach(println)
    val repartitionRDD: RDD[(Int, String)] = nameRDD2.repartition(6)
    val namerepar: RDD[(Int, (Int, String))] = repartitionRDD.mapPartitionsWithIndex(mapPartitionsIndex4,true)
    namerepar.collect.foreach(println)


  }
  def mapPartitionsIndex3(i1:Int,iter:Iterator[String]):Iterator[(Int,String)]={
    var res = List[(Int,String)]()
    while (iter.hasNext) {
      val name = iter.next()
      res = res. ::(i1,name)
    }
    res.iterator
  }
  def mapPartitionsIndex4(i1:Int,iter:Iterator[(Int,String)]):Iterator[(Int,(Int,String))]={
    var res = List[(Int,(Int,String))]()
    while (iter.hasNext) {
      val tuple: (Int, String) = iter.next()
      res = res.::(i1,tuple)

    }
    res.iterator
  }

}
