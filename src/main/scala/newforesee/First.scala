package newforesee

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 1.使用两种方式创建RDD并将数据进行处理，统计单词出现的数量
  */
object First {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)
    //第一种创建RDD方式
    val file = sc.textFile("D://a.txt")

    //第二种创建RDD方式
//    val arr = Array("hello world","hello monica","hello sky")
//    val lines = sc.parallelize(arr)

    val words = file.flatMap(_.split(" ")).map((_,1))
    val result: RDD[(String, Int)] = words.reduceByKey(_+_)
    result.foreach(println)
  }
}

/**
  *2.val datas = Array(1, 2, 3, 7, 4, 5, 8)
  * 将上面的数据进行取偶
  * val hashMap = mutable.HashMap(
  * 2->"zhangsan",4->"lisi",8->"wangwu")
  * 并使用Mappartitions算子实现取hashMap的value值，进行显示打印
  */
object Second {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)

    val datas = Array(1, 2, 3, 7, 4, 5, 8)
    val hashMap = mutable.HashMap(2->"zhangsan",4->"lisi",8->"wangwu")
    val dataSet = sc.parallelize(datas)

    val filted = dataSet.filter(_%2==0)
    val result: RDD[String] = filted.mapPartitions(m => {
      var list = mutable.LinkedList[String]()
      while (m.hasNext) {
        list ++= hashMap.get(m.next())
      }
      list.iterator
    })
    result.foreach(println)

  }
}

/**
  * 3. val arr = Array("hello", "world", "hello", "spark", "hello", "hive", "hi", "spark")
  * 将上述数据进行去重，然后统计去重后的数据取前2个进行显示
  */
object Third {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)
    
    val arr = Array("hello", "world", "hello", "spark", "hello", "hive", "hi", "spark")
    val words = sc.parallelize(arr)
    val wordDist: RDD[String] = words.distinct()
    wordDist.top(2).foreach(println)

  }
}

/**
  * 4.val arr = Array("dog cat gnu","salmon rabbit","turkey wolf","bear bee")
  * 用groupByKey和reduceByKey两种方式实现单词计数的统计
  */
object Fourth{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array("dog cat gnu","salmon rabbit","turkey wolf","bear bee")
    val lines: RDD[String] = sc.parallelize(arr)
    val words = lines.flatMap(_.split(" "))
    val tuples = words.map((_,1))

    //使用groupByKey
    val grouped: RDD[(String, Iterable[Int])] = tuples.groupByKey()
    val counts: RDD[(String, Int)] = grouped.map(x=>(x._1,x._2.sum))
    counts.collect().foreach(println)

    //使用reduceByKey
    val count: RDD[(String, Int)] = tuples.reduceByKey(_+_)
    count.collect().foreach(println)
    sc.stop()
  }
}