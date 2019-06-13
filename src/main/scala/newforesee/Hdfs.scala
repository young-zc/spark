package newforesee

import com.google.gson.{JsonObject, JsonParser}

/**
  * xxx
  * creat by newforesee 2019-03-08
  */
object Hdfs {
  def main(args: Array[String]): Unit = {
    gson("{\"host\":\"td_test\",\"ts\":1486979192345,\"device\":{\"tid\":\"a123456\",\"os\":\"android\",\"sdk\":\"1.0.3\"},\"time\":1501469230058}")

  }

  def gson(str: String) = {
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    println(obj.get("device"))
    println(obj.get("ts"))
  }

}
