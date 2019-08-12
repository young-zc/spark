package newforesee.test

import org.apache.log4j.Logger
import vegas._

object Test_vegas {
  def main(args: Array[String]): Unit = {
    val plot = Vegas("Results").
      withData(
        Seq(
          Map("x" -> 1, "y" -> 1,"Origin"->"a"),
          Map("x" -> 2, "y" -> 2,"Origin"->"b"),
          Map("x" -> 3, "y" -> 3,"Origin"->"c")
        )
      ).
      encodeX("x", Nominal).
      encodeY("y", Nominal).
      encodeColor(field="Origin", dataType= Nominal).
      mark(Point)
    plot.show

  }
}
