package com.company

import scala.collection.mutable.ListBuffer

/**
 * Created by HM on 2015/11/5.
 */
object testInScala {
  def main(args: Array[String]) {
    val name = "江苏省苏州市沧浪区中天集团有限公司"

//    name.length
//
//    val DIC = "有限公司"
//
//    println(name.replace("有限公司",""))

//    var i = 10
//    for(i <- 10 to 1){
//      println(i)
//
//    }
//
//    while(i>0 ){
//      println(i)
//      i= i-1
//    }

//    var i = 0
//    while(i<=10){
//      println(i)
//      i= i+1
//    }

//    for(i <- 10 to 23){
//      println(i)
//    }

    val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
    val dayss = ListBuffer("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

//    println(dayss.last)

    val result = days(2)+dayss(3)
    println(result)



  }

}
