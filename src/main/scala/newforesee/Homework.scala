package newforesee
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable



  object work {
    //spark的RDD算子练习
    //1.使用两种方式创建RDD并将数据进行处理,统计单词出现的数量
    //
    //2.val datas = Array(1, 2, 3, 7, 4, 5, 8)
    //将上面的数据进行取偶
    //val hashMap = mutable.HashMap(2 -> "zhangsan", 4 -> "lisi", 8 -> "wangwu")
    //并使用Mappartitions算子实现取hashMap的value值,进行显示打印
    //
    //3.val arr = Array("hello", "world", "hello", "spark", "hello", "hive", "hi", "spark")
    //将上述数据进行去重，然后统计去重后的数据取前2个进行显示
    //
    //4.val arr = Array("dog cat gnu", "salmon rabbit", "turkey wolf", "bear bee")
    //用groupByKey和reduceByKey两种方式实现单词计数的统计

    def work1(): Unit = {
      val conf = new SparkConf().setAppName("work1").setMaster("local")
      val sc = new SparkContext(conf)

      val rdd11 = sc.parallelize(Array("张三", "李四", "王二", "麻子"))
      val rdd12 = sc.textFile("/usr/local/data.txt")

      val words: RDD[(String, Int)] = rdd11.map((_, 1))
      val result: RDD[(String, Int)] = words.reduceByKey(_ + _)
      result.collect().foreach(x => println(x))
    }

    def work2(): Unit ={
      val conf: SparkConf = new SparkConf().setAppName("work2").setMaster("local")
      val sc = new SparkContext(conf)

      val datas = Array(1, 2, 3, 7, 4, 5, 8)
      val data: Array[Int] = datas.filter(x => x%2==0)
      data.foreach(println)
      val idRDD = sc.parallelize(data)
      val hashMap = mutable.HashMap(2 -> "zhangsan", 4 -> "lisi", 8 -> "wangwu")
      val userID: RDD[String] = idRDD.mapPartitions(m => {
        var list = mutable.LinkedList[String]()
        while (m.hasNext) {
          val id = m.next()
          val name = hashMap.get(id)
          list ++= name
        }
        list.iterator
      })
      userID.collect.foreach(println)
    }

    def work3(): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("work3").setMaster("local")
      val sc = new SparkContext(conf)

      val arr = Array("hello", "world", "hello", "spark", "hello", "hive", "hi", "spark")
      val lineRDD: RDD[String] = sc.parallelize(arr)
      val result: Array[String] = lineRDD.distinct().take(2)
      result.foreach(println)
    }

    def work4(): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("work4").setMaster("local")
      val sc = new SparkContext(conf)
      val arr = Array("dog cat gnu", "salmon rabbit", "turkey wolf", "bear bee")
      val arrRDD: RDD[String] = sc.parallelize(arr)

      val words: RDD[String] = arrRDD.flatMap(_.split(" "))
      val word: RDD[(String, Int)] = words.map((_, 1))

      val gword: RDD[(String, Iterable[Int])] = word.groupByKey()
      gword.foreach(f => (f._1,f._2.sum))

      val rword: RDD[(String, Int)] = word.reduceByKey(_+_)
      rword.foreach(println)
    }

    def main(args: Array[String]): Unit = {
      work1()
      work2()
      work3()
      work4()
    }

}
