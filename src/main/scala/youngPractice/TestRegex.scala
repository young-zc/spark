package youngPractice

import scala.util.matching.Regex

object TestRegex {
  def main(args: Array[String]): Unit = {

    val numberPattern: Regex = "[0-9]".r
    //result:Password OK!
    numberPattern.findFirstMatchIn("awesome2password") match {
      case Some(_) => println("Password OK!")

      case None => println("Password must contain a number")
    }

    //result:Some(8)
    println(numberPattern.findFirstIn("sun88moon"))

    //result:password ok
    numberPattern.findFirstIn("awesome2password") match {
      case Some(_) => println("password ok")

      case None => println("Password must contain a number")
    }




    val keyValPattern: Regex = "([0-9a-zA-Z-#() ]+): ([0-9a-zA-Z-#() ]+)".r
    val input: String =
      """
        |background-color: #A03300;
        |background-image: url(img/header100.png);
        |background-position: top center;
        |background-repeat: repeat-x;
        |background-size: 2160px 108px;
        |margin: 0;
        |height: 108px;
        |width: 100%;
      """.stripMargin
    for (patternMatch <- keyValPattern.findAllMatchIn(input)) {
      println(s"key: ${patternMatch.group(1)} value: ${patternMatch.group(2)}")
    }


  }

}
