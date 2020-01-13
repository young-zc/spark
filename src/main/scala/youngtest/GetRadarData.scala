package youngtest

import java.util.Properties

import newforesee.test.Test

object GetRadarData extends Test{
  override def run(): Unit = {

    val properties = new Properties()
    properties.setProperty("url","jdbc:hnlowoin")
    properties.setProperty("username","test")
    properties.setProperty("password","abcd")
    println(properties.getProperty("url"),properties.getProperty("username"),properties.getProperty("password"))
    //spark.read.jdbc(properties.getProperty("url"),"",properties)

  }
}
