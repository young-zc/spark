package youngPractice

object TestForExpr {
  def main(args: Array[String]): Unit = {

    foo(10,10)

    println(">>>>>>>>>>>>>>>>>>foo2")
    foo2(10,10).foreach(println)

    println(">>>>>>>>>>>>>>>>>>foo2>>>>>>>>>>>>>>>>>>")
    foo2(10,10) foreach{
      case (i, j) => println(i,j)
    }

  }

  def foo(n: Int, v: Int) = {
    for (i <- 0 until n; j <- i until n if i+j==v)
      //println(s"($i,$j)")
      println(i,j)
  }

  //yield 会将内容都存入list中并返回
  def foo2(n: Int, v: Int) ={
    for (i <- 0 until n; j <- i until n if i+j==v)
      yield (i, j)
  }

}
