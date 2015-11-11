package com.join

import scala.collection.mutable.ListBuffer

/**
 * Created by HM on 2015/10/14.
 */
object test {
  def main(args: Array[String]) {


    val listB = new ListBuffer[String]

    for( l <- 'a' to 'e'){
      listB.append(l.toString)
    }



    val list = listB.toList

//    list.map(_+1)

//    list.foreach(println _)

    println(!list.isEmpty)

//    for( l <- list){
//      println(l)
//    }



  }

}
