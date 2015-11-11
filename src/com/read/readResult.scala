package com.read

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by HM on 2015/10/12.
 */
object readResult {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("readResult").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/ada/files/result1.txt")

    val result = rdd1.map(line =>{
      val fileds = line.split(",")
      (fileds(0), fileds(1))
    })

  }

}
