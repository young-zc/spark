package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 公司部门整合
  */
object Coalesces {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Coalesces").setMaster("local")
    val sc = new SparkContext(conf)
    //Coalesces算子,是将RDD的partition缩减减少
    //将一定的数据压缩到更少的partition中去

    //建议使用场景,配合filter算子使用
    //使用filter算子过滤掉很多数据以后,比如30%数据,出现了很多partition中的数据不均匀的情况
    //建议使用Coalesces算子,压缩partition数量
    //从而让各个partition中的数据更加紧凑


    //公司原有6各部门
    //但是裁员之后有的部门中的人员减少
    //出现了部门人员分配不均匀的现象,此时进行部门整合操作,将不同的部门员工进行整合
    val list = List("张三","李四","王五","赵六","赵钱孙","吴正旺","冯陈楚","蒋沈晗","朱亲友","徐博文")
    //6个分区表示六个部门
    val nameRDDs = sc.parallelize(list,6)
    //查看之前的部门分配情况
    val namePartitions: RDD[(Int, String)] = nameRDDs.mapPartitionsWithIndex(mapPartitionsIndex,preservesPartitioning = true)
    //打印之前的人员在哪个部门
    namePartitions.collect.foreach(println)
    /**
      * (0,张三)
      * (1,王五)
      * (1,李四)
      * (2,赵钱孙)
      * (2,赵六)
      * (3,吴正旺)
      * (4,蒋沈晗)
      * (4,冯陈楚)
      * (5,徐博文)
      * (5,朱亲友)
      */
    //接下来调用coalesce算子将分区减少为3
    val namePartition2: RDD[(Int, String)] = namePartitions.coalesce(3)
    //查看重新分区后的人员调整
    val namePartition3: RDD[(Int, (Int, String))] = namePartition2.mapPartitionsWithIndex(mapPartitionsIndex2,true)
    namePartition3.foreach(println)
  }
  def mapPartitionsIndex(i1:Int,iter:Iterator[String]):Iterator[(Int,String)]={
      var res = List[(Int,String)]()
    while (iter.hasNext) {
      val name = iter.next()
      res = res. ::(i1,name)
    }
    res.iterator
  }
  def mapPartitionsIndex2(i1:Int,iter:Iterator[(Int,String)]):Iterator[(Int,(Int,String))]={
    var res = List[(Int,(Int,String))]()
    while (iter.hasNext) {
      val tuple: (Int, String) = iter.next()
      res = res.::(i1,tuple)

    }
    res.iterator
  }

}
