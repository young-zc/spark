package youngPractice

import newforesee.test.Test
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

object TestDateDiff extends Test{

  import spark.implicits._
  override def run(): Unit = {

    val bdate = Seq(
      (1,"2020-03-05"),
      (1,"2020-03-10"),
      (1,"2020-03-16"),
      (1,"2020-03-26"),
      (2,"2020-03-10"),
      (2,"2020-03-13"),
      (2,"2020-03-13"),
      (2,"2020-03-21"),
      (2,"2020-03-23"))
    val edate = Seq(
      (1,"2020-03-18"),
      (1,"2020-03-10"),
      (2,"2020-03-10"),
      (2,"2020-03-20"))

    val bdf: DataFrame = spark.sparkContext.makeRDD(bdate).toDF("id","bdate")
    val edf: DataFrame = spark.sparkContext.makeRDD(edate).toDF("id","edate")
    bdf.join(edf,edf("id")===bdf("id") && datediff(edf("edate"),bdf("bdate"))>=0 && datediff(edf("edate"),bdf("bdate"))<=5).show()

    val tdate = Seq(
      (1,"2020-03-05",""),
      (1,"2020-03-10",""),
      (1,"2020-03-16",""),
      (1,"2020-03-26",""),
      (2,"2020-03-10",""),
      (2,"2020-03-13",""),
      (2,"2020-03-13",""),
      (2,"2020-03-21",""),
      (2,"2020-03-23",""),
      (1,"","2020-03-18"),
      (1,"","2020-03-10"),
      (2,"","2020-03-10"),
      (2,"","2020-03-20"),
      (1,"2020-04-18","2020-04-20"),
      (1,"2020-03-03","2020-03-06"),
      (2,"2020-05-20","2020-05-20"))
    val testdf: DataFrame = spark.sparkContext.makeRDD(tdate).toDF("id","bdate","edate")
    val df1: DataFrame = testdf.select("id","bdate")
    val df2: DataFrame = testdf.select("id","edate").withColumnRenamed("edate","eedate")
    df1.join(df2,df1("id")===df2("id") && datediff(df2("eedate"),df1("bdate"))>=0 && datediff(df2("eedate"),df1("bdate"))<=5).show()
    /*bdf.show()
    bdf.withColumn("b-e",datediff($"bdate",$"edate")).show()
    bdf.withColumn("e-b",datediff($"edate",$"bdate")).show()*/

    //val edf: DataFrame = spark.sparkContext.makeRDD(edate).toDF("edate")

    //bdf.withColumn("b-e",datediff(bdf("bdate"),edf("edate"))).show()
    //edf.withColumn("e-b",datediff(edf("edate"),bdf("edate"))).show()
  }
}
