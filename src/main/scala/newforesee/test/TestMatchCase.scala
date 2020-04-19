package newforesee.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object TestMatchCase extends Test{
  override def run(): Unit = {

    Seq(
      "D:\\workspace\\IDEA\\spark_examples\\src\\a.json",
      "D:\\workspace\\IDEA\\spark_examples\\src\\b.json",
      "D:\\workspace\\IDEA\\spark_examples\\src\\c.json"
    ).foreach((path: String) => {
      path match {
        case "D:\\workspace\\IDEA\\spark_examples\\src\\a.json" =>
          spark.read.json(path)
            .withColumn("u_time",current_timestamp())
            .show(false)

        case "D:\\workspace\\IDEA\\spark_examples\\src\\b.json" =>
          spark.read.json(path)
            .withColumn("companyId",lit(6))
            .show()

        case _ =>
          spark.read.json(path)
            .show()
      }

    })
  }
}
