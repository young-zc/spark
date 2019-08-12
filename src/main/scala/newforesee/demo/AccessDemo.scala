package newforesee.demo

import newforesee.test.Test
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * xxx
  * creat by newforesee 2019-08-12
  */
object AccessDemo extends Test {

  import spark.implicits._

  override def run(): Unit = {
    /**
      * {"id": 1, "name": "Technical Department","acc": 1}
      * {"id": 2, "name": "Financial Department","acc": 3}
      * {"id": 3, "name": "HR Department","acc": 6}
      * {"id": 4, "name": "起动机","acc": 12}
      * {"id": 5, "name": "尿素泵","acc": 4}
      * {"id": 6, "name": "后处理","acc": 10}
      */

    /**
      * {"roles":"CCI","acc":1}
      * {"roles":"DCEC","acc":2}
      * {"roles":"CCG","acc":4}
      * {"roles":"CES","acc":8}
      */
    val dim_df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/demo/Roles.json")
    dim_df.createOrReplaceTempView("acc_dim")
    val data_df: DataFrame = spark.read.json("/Users/newforesee/Intellij Project/Spark/src/main/scala/newforesee/demo/datas.json")
    data_df
      .withColumn("bin", bin($"acc"))
      //.show()
      .createOrReplaceTempView("data")

    spark.sql(
      """
        |select d.id,d.name
        | from data d,acc_dim a
        | where ((b.acc & (select acc from acc_dim where roles="DCEC")as m )= m)
      """.stripMargin).show()
    spark.sql("select * from data,acc_dim where (acc & 8)=8 ").show()


  }
}
