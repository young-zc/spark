package dongliang


import java.util.Properties

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * creat by newforesee 2018/11/10
  */
object ReadParqutt {
  def main(args: Array[String]): Unit = {
    val pps: Properties = System.getProperties
    pps.setProperty("file.encoding", "UTF-8")
    val filePath = "/Users/newforesee/Intellij Project/Spark/src/main/scala/dongliang/Build_Data_200905.csv"
    val session: SparkSession = SparkSession.builder.appName("ss").master("local[2]").getOrCreate()

    session.read.parquet()
    val rddstring: RDD[String] = session.sparkContext.hadoopFile(filePath, classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text]).map(( pair: (LongWritable, Text)) => new String(pair._2.getBytes, 0, pair._2.getLength, "UNICODE"
      )
    )
    rddstring.foreach(println)



  }


}
