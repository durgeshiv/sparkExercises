package main.scala.com.durgesh.software.sparkExercises

import scala.annotation.tailrec

object fact extends App {


  def factorial(n: Int): Int = {
    if (n == 0) return 1
    else
      return n * factorial(n - 1)
  }

  println("factorial rec :: "+ factorial(5))


  def fact(n: Int) = {

    @tailrec
    def factTailRec(n:Int, acc: Int): Int = {

      if (n <= 1)
        acc
      else
        factTailRec(n - 1, n * acc)

    }
      factTailRec(n,1 )
    }

}
