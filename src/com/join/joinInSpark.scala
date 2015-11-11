package com.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * Created by HM on 2015/10/9.
 */
object joinInSpark {


  def joinForMachine(rdd1: RDD[String], rdd2: RDD[String], resultPath: String): Unit = {

    val news = rdd1.map(line => {
      val fileds = line.split("::")
      if (fileds.length == 3) {
        (fileds(0), fileds(1)) //<newsPath,processedNews>
      } else {
        ("k", "k") //if the recode is null
      }
    })

    val listN = news.toArray()

    val companyNames = rdd2.map(line => {
      val fileds = line.split(",")
      if (fileds.length == 3) {
        (fileds(0), fileds(2)) //<companyID,simplifiedName>
      } else {
        ("n", "n")
      }
    })


    val result2 = companyNames.map(f => {
      val list = new ListBuffer[String]()
      for (n <- listN) {
        if (n._2.contains(f._2)) {
          //          println("news:" + n._2 + "name:" + f._2)
          list.append(n._1)
        }
      }
      (f._1, list.toList)
    }).filter(f => !f._2.isEmpty)

    //    result2.foreach(f => println(f))
    //    println(result2.count())
    result2.repartition(1).saveAsTextFile(resultPath)

  }

  def joinForMachineRepartion(rdd1: RDD[String], rdd2: RDD[String], resultPath: String): Unit = {

    val news = rdd1.repartition(12).map(line => {
      val fileds = line.split("::")
      if (fileds.length == 3) {
        (fileds(0), fileds(1)) //<newsPath,processedNews>
      } else {
        ("k", "k") //if the recode is null
      }
    })

    val listN = news.toArray()

    val companyNames = rdd2.repartition(12).map(line => {
      val fileds = line.split(",")
      if (fileds.length == 3) {
        (fileds(0), fileds(2)) //<companyID,simplifiedName>
      } else {
        ("n", "n")
      }
    })


    val result2 = companyNames.map(f => {
      val list = new ListBuffer[String]()
      for (n <- listN) {
        if (n._2.contains(f._2)) {
          //          println("news:" + n._2 + "name:" + f._2)
          list.append(n._1)
        }
      }
      (f._1, list.toList)
    }).filter(f => !f._2.isEmpty)

    result2.repartition(1).saveAsTextFile(resultPath)

  }


  //rdd1:news, rdd2:companyName
  def joinForMan(rdd1: RDD[String], rdd2: RDD[String], resultPath: String): Unit = {

    val news = rdd1.map(line => {
      //因为这里写错了，写成了rdd2，浪费太多时间。
      val fileds = line.split("::")
      if (fileds.length == 3) {
        (fileds(2), fileds(1)) //<completeNews,processedNews>
      } else {
        ("k", "k")
      }
    })

    val listN = news.toArray()
    //    listN.foreach(println _)

    val companyNames = rdd2.map(line => {
      val fileds = line.split(",")
      if (fileds.length == 3) {
        (fileds(1), fileds(2)) //<completeName,simplifiedName>
      } else {
        ("n", "n")
      }
    })

    //    companyNames.foreach(println _)

    val result = companyNames.map(f => {
      val list = new ListBuffer[String]()
      for (n <- listN) {
        if (n._2.contains(f._2)) {
          //          println("news:" + n._2 + "name:" + f._2)
          list.append(n._1)
        }
      }
      (f._1, list.toList)
    }).filter(f => !f._2.isEmpty)

    //    result.foreach(f => println(f))
    //    println(result.count())

    result.repartition(1).saveAsTextFile(resultPath)
  }

  //rdd1:news, rdd2:companyName
  def joinForManRepartion(rdd1: RDD[String], rdd2: RDD[String], resultPath: String): Unit = {

    val news = rdd1.map(line => {
      val fileds = line.split("::")
      if (fileds.length == 3) {
        (fileds(2), fileds(1)) //<completeNews,processedNews>
      } else {
        ("k", "k")
      }
    })

    val listN = news.toArray()    //news have NUM partitions

    val companyNames = rdd2.map(line => {
      val fileds = line.split(",")
      if (fileds.length == 3) {
        (fileds(1), fileds(2)) //<completeName,simplifiedName>
      } else {
        ("n", "n")
      }
    })

    val result = companyNames.map(f => {      //companyNames have NUM partitions
      val list = new ListBuffer[String]()
      for (n <- listN) {
        if (n._2.contains(f._2)) {
          println("news:" + n._2 + "name:" + f._2)
          list.append(n._1)
        }
      }
      (f._1, list.toList)
    }).filter(f => !f._2.isEmpty)

    result.repartition(1).saveAsTextFile(resultPath)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("readNews")
      .set("spark.executor.instances", "8")
      .set("spark.executor.cores", "5")
      .set("spark.executor.memory", "10g")
      .set("spark.driver.maxResultSize", "2g")
      .set("spark.driver.memory", "2g") //.setMaster("local")

    val sc = new SparkContext(conf)

    //    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/cut2/news2_cut_ac")
    //

//    val rdd2 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/lastResult.txt").repartition(40)
////
//    for (l <- 'a' until 'n') {
//      val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/cut2_200/news2_cut_a" + l).repartition(20)
//      joinForMachineRepartion(rdd1, rdd2, "hdfs://192.168.131.192:9000/user/humeng/project1/cut2_200/joinResultForMachine2" + l)
//      joinForManRepartion(rdd1, rdd2, "hdfs://192.168.131.192:9000/user/humeng/project1/cut2_200/joinResultForMan2" + l)
//    }
//
//    for (l <- 'a' until 'o') {
//      val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/cut3_200/news3_cut_a" + l).repartition(20)
//      joinForMachineRepartion(rdd1, rdd2, "hdfs://192.168.131.192:9000/user/humeng/project1/cut3_200/joinResultForMachine3" + l)
//      joinForManRepartion(rdd1, rdd2, "hdfs://192.168.131.192:9000/user/humeng/project1/cut3_200/joinResultForMan3" + l)
//    }

    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/cut2_200/news2_cut_aa").repartition(30)
    val rdd2 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/lastResult.txt").repartition(40)
    joinForManRepartion(rdd1, rdd2, "hdfs://192.168.131.192:9000/user/humeng/project1/joinResultForManNews_cut_aaTest30P40P8E5C")




    sc.stop()
  }
}
