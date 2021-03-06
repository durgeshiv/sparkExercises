package main.scala.com.durgesh.software.sparkExercises

import org.apache.spark.sql.catalyst.util.TimestampFormatter


object fibonacci extends App {

  def nth(n: Int): Long = {

    def loop(n: Int): Long = n match {
      case 0 | 1 => n
      case _ => nth(n -1 ) + nth(n - 2 )
    }
    require( n >= 0)
    loop(n)


  }
 println("fibonacci :: " + nth(8))

  case class abc(id: String, time: TimestampFormatter)


}
