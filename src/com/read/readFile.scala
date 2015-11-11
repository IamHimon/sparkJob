package com.read

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by HM on 2015/10/9.
 */
object readFile {
  def main(args: Array[String]) {

    val start = System.currentTimeMillis()
    if(args.length!=1){
      println("read file:<companyName>")
      return
    }

    val conf = new SparkConf().setAppName("readFile")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile(args(0))
//    val rdd2 = sc.textFile(args(1))

    val list1 = rdd1.toArray()
//    val list2 = rdd2.toArray()

    for(l <- list1){
      println(l.split(",")(1))
    }

//    for(s <- list2){
//      println(s.split("::")(1))
//    }

    val end = System.currentTimeMillis()
    println("consume:"+(end-start)+"second")

  }

}
