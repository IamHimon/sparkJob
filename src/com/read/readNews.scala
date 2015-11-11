package com.read

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * Created by HM on 2015/10/9.
 */
object readNews {
  def main(args: Array[String]) {

    val start = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("readNews") //.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/ada/files/news.txt")
    val rdd2 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/ada/files/result1.txt")

    val news = rdd1.map(line => {
      val fileds = line.split("::")
      (fileds(0), fileds(1)) //<newsPath,processedNews>
    })

    val listN = news.toArray()

    val companyNames = rdd2.map(line => {
      val fileds = line.split(",")
      (fileds(0), fileds(2)) //<companyID,simplifiedName>
    })


    val result2 = companyNames.map(f => {
      val list = new ListBuffer[String]()
      for (n <- listN) {
        if (n._2.contains(f._2)) {
//          print("news:" + n._2 + "name:" + f._2)
          list.append(n._1)
        }
      }
      (f._1, list)
    }).filter(f => f._2.size != 0)

    result2.foreach(f => println(f))
    result2.saveAsTextFile("hdfs://192.168.131.192:9000/user/humeng/ada/files/joinResult1.txt")

    val end = System.currentTimeMillis()
    println("consume:" + (end - start) / 1000 + "second")
    sc.stop()
  }

}
