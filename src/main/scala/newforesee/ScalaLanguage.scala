package newforesee

import scala.collection.mutable

/**
  * xxx
  * creat by newforesee 2019-07-26
  */
object ScalaLanguage {
  def main(args: Array[String]): Unit = {
    val arr: List[String] = List("a", "b", "c", "a", "d", "c", "a")
    println(
//      list.map((_, 1))
//        .groupBy(_._1)
//        .map(x => {(x._1, x._2.size)})
//        .mkString("\001")
//        .replaceAll("->","*")
//        .split("\001")
//        .toList
      arr.map((_: String, 1))
        .groupBy((_: (String, Int))._1)
        .map((x: (String, List[(String, Int)])) => {(x._1, x._2.size)})
        .mkString(",")
        .replaceAll("->","*")
    )
  }

}
