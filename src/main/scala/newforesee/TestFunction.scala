package newforesee

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object TestFunction {
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    /**
      * 1,A,X,a
      * 1,B,Y,b
      * 2,A,X,a
      * 2,B,Y,b
      * 3,A,X,a
      * 3,B,Y,b
      * 4,A,X,a
      * 4,B,Y,b
      */
    val ds: Dataset[String] = spark.read.textFile("src/testdata")
    val rowrdd: RDD[Row] = ds.rdd.map((l: String) => {
      val strings: Array[String] = l.split(",")
      Row(strings(0), strings(1), strings(2), strings(3))
    })
    val cloStructType: StructType = new StructType()
      .add("id", StringType)
      .add("clo1", StringType)
      .add("clo2", StringType)
      .add("clo3", StringType)
    val df: DataFrame = spark.createDataFrame(rowrdd, cloStructType)
    df.show()
    val df2: DataFrame = df.groupBy($"id").agg(
      collect_list($"clo1") as "clo1",
      collect_list($"clo2") as "clo2",
      collect_list($"clo3") as "clo3"
    )
        .withColumn("new1",ListReduceCount($"clo1"))
        .withColumn("new2",ListReduceCount($"clo2"))
        .withColumn("new3",ListReduceCount($"clo3"))
    df2.show()

    val df3: DataFrame = buildPartDescription(df2)

    println("df3 >>>>>>>>>>>>>>> ")
    println()
    df3.show()
    val df4: DataFrame = df2.withColumn("newclos", ZipFaultCodeWithLOC($"clo1", $"clo2", $"clo3"))
    df4.show()


  }

  def buildPartDescription(df: DataFrame): DataFrame = {
    val origin: DataFrame = df.select("id")
    .withColumn("new_id", monotonically_increasing_id)
    println("origin >>>>>>>>>>>>>>> ")
    origin.show()

    val explode_PART_NUM: DataFrame = df.select($"id", explode($"clo1") as "clo1_1")
      .withColumn("new_id", monotonically_increasing_id)

    println("explode_PART_NUM >>>>>>>>>>>>>>> ")
    explode_PART_NUM.show()

    val explode_PART_DESC: DataFrame = df.select($"id" as "id_1", explode($"clo2") as "clo2_1")
      .withColumn("new_id", monotonically_increasing_id)

//    val explode_PART_PRI: DataFrame = df.select($"id" as "id_2", explode($"clo3") as "clo3_1")
//      .withColumn("new_id", monotonically_increasing_id)

    println("explode_PART_DESC >>>>>>>>>>>>>>> ")
    explode_PART_DESC.show()

    origin.join(explode_PART_NUM, Seq{"new_id"}, "left")
          .join(explode_PART_DESC, Seq{"new_id"}, "left").drop("new_id")
//    val tmp1: DataFrame = explode_PART_NUM.join(explode_PART_DESC, Seq {"new_id"}, "left")
//      .join(explode_PART_PRI,Seq{"new_id"}, "left")
//        .select("id","clo1_1","clo2_1","clo3_1")

//    tmp1.show()
//    origin.join(tmp1, Seq {
//      "id"
//    }, "left").drop("new_id")

  }

  def ZipFaultCodeWithLOC: UserDefinedFunction = udf {
    (fault_code_list: Seq[String], latitude_list: Seq[String], longitude_list: Seq[String]) => {
      val values: Seq[(String, (String, String))] = fault_code_list.zip(latitude_list.zip(longitude_list))
      values.map((value: (String, (String, String))) => (value._1, value._2._1, value._2._2))
    }
  }

  /**
    * Example:
    *   before : ("a", "b", "c", "a", "d", "c", "a")
    *   after :  (b * 1, d * 1, a * 3, c * 2)
    */
  val ListReduceCount: UserDefinedFunction = udf {
    arr: mutable.WrappedArray[String] =>{
      arr.map((_: String, 1))
        .groupBy((_: (String, Int))._1)
        .map((x: (String, mutable.WrappedArray[(String, Int)])) => {(x._1, x._2.size)})
        .mkString("\u0001")
        .replaceAll("->","*")
        .split("\u0001")
        .toList
    }
  }

}
