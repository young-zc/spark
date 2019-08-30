package newforesee.test

import newforesee.Utils.Util
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Log

/**
  * xxx
  * creat by newforesee 2019-08-12
  */
trait Test {
  val logger: Logger = Logger.getLogger(this.getClass)
  val spark: SparkSession = Util.getSpark(this.getClass)
  private val start: Long = System.currentTimeMillis()
  def main(args: Array[String]): Unit = {
    run()

    println(s"Job Finished in %d seconds".format((System.currentTimeMillis()-start)/1000))
  }
  def run()

}
