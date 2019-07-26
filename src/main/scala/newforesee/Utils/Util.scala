package newforesee.Utils

import org.apache.spark.sql.SparkSession

/**
  * xxx
  * creat by newforesee 2019-07-17
  */
object Util {
  def getSpark(clazz: Any): SparkSession ={

    SparkSession.builder().master("local").appName(clazz.getClass.getSimpleName).getOrCreate()
  }

}
