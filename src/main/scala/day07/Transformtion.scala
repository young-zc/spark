package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformtion {
  //利用map算子实现每个算子*2
  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1, 2, 3, 4, 5)
    val numberRDD: RDD[Int] = sc.parallelize(arr, 1)
    val maps: RDD[Int] = numberRDD.map(x => x * 2)
    maps.foreach(println)
  }

  //通过过滤偶数
  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numRdd: RDD[Int] = sc.parallelize(arr)
    val filterRdd: RDD[Int] = numRdd.filter(num => num % 2 == 0)
    filterRdd.foreach(println)
  }

  //通过flatMap切分压平
  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array("hello you", "hello me", "hello word")
    val lines: RDD[String] = sc.parallelize(arr)
    val words: RDD[String] = lines.flatMap(f => f.split(" "))
    words.foreach(println)
  }

  //按照Key分组
  def groupbyKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val arr: Array[(String, Int)] = Array(("class1", 80), ("class2", 75), ("class1", 90), ("class2", 60))
    val classScores: RDD[(String, Int)] = sc.parallelize(arr)
    val groupby: RDD[(String, Iterable[Int])] = classScores.groupByKey()
    groupby.foreach(f => {
      println(f._1)
      f._2.foreach(println)
    })
  }
//按照key进行聚合
  def reduceByKey(): Unit ={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val arr: Array[(String, Int)] = Array(("class1", 80), ("class2", 75), ("class1", 90), ("class2", 60))
    val classScores = sc.parallelize(arr)
    val Scores: RDD[(String, Int)] = classScores.reduceByKey(_+_)
    Scores.foreach(println)

  }
  //
  def sortByKey(): Unit ={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val arr= Array((80,"linda"), (90,"newforesee"), (60, "wety"))
    val sor = sc.parallelize(arr)
    val result = sor.sortByKey(false)
    result.foreach(f => println(f._1+"::" +f._2))
  }
  //join操作
  def join(): Unit ={
    val conf = new SparkConf().setAppName("join").setMaster("local")
    val sc = new SparkContext(conf)
    val student = Array((1,"leo"),(2,"linda"),(3,"newforesee"))
    val scores = Array((1,99),(2,98),(3,100))
    val studentRDD = sc.parallelize(student)
    val scoresRDD: RDD[(Int, Int)] = sc.parallelize(scores)
    val studentScores: RDD[(Int, (String, Int))] = studentRDD.join(scoresRDD)
    studentScores.foreach(f => {
      println("student ID: " + f._1)
      println("name: "+f._2._1)
      println("student scores: "+f._2._2)
    })
  }

  def cogroup(): Unit ={
    val conf = new SparkConf().setAppName("cogroup").setMaster("local")
    val sc = new SparkContext(conf)
    val student = Array((1,"leo"),(2,"linda"),(3,"newforesee"))
    val scores = Array((1,99),(2,98),(3,100))
    val studentRDD = sc.parallelize(student)
    val scoresRDD: RDD[(Int, Int)] = sc.parallelize(scores)
    val studentScores: RDD[(Int, (Iterable[String], Iterable[Int]))] = studentRDD.cogroup(scoresRDD)
    studentScores.foreach(f => {
      println("student ID: " + f._1)
      println("name: "+f._2._1)
      println("student scores: "+f._2._2)
    })
  }

  def main(args: Array[String]): Unit = {
    //map()
    //filter()
    //flatMap()
    //groupbyKey()
    //reduceByKey()
    //sortByKey()
    //join()
    cogroup()

  }

}
