package youngPractice

object TestInterPolater {

  def main(args: Array[String]): Unit = {

    val language: String = "chaoyang"
    val tStr: String = "love\nyou"

    println(s"hello,$language.")
    println(s"Hi,${language}.")
    println(raw"I $tStr")
    println(raw"I love\nyou")

  }

}
