package newforesee

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object WC {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("WC").master("local").getOrCreate()
    val lines: Dataset[String] = spark.read.textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt")

    val result: RDD[(String, Int)] = lines.rdd.flatMap((line: String) => {
      line.replaceAll("[^a-zA-Z0-9]+", " ").split(" ")
    }).map((_, 1)).reduceByKey(_ + _).sortBy(_._2,false)
    result.saveAsTextFile("Result.txt")
//    spark.close()
    spark.read.parquet("")
  }


}
