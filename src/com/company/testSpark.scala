package com.company

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Created by HM on 2015/11/5.
 */
object testSpark {

  def initialProcess(completeName: String, dic: Array[String]): String = {
    var result = new String
    breakable {
      for (line <- dic) {
        val temp = completeName.replace(line, "")
        if (!temp.equals(completeName)) {
          result = temp
          break;
        } else {
          result = completeName
        }
      }
    }
    return result
  }

  def startSeg(processedName: String, pdic: Array[String]): List[String] = {
    val result = new ListBuffer[String]
    var map = new HashMap[String, List[String]]
    val retained = new ListBuffer[String]
    val removed = new ListBuffer[String]

    var temp = new String

    breakable {
      for (i <- 0.to(processedName.length)) {
        temp = processedName.substring(0, i)
        if (pdic.contains(temp)) {
          break()
        }
      }
    }
    //
    //    var i = 0
    //    var check = false
    //    while (i < processedName.length && !check) {
    //      temp = processedName.substring(0, i)
    //      if (pdic.contains(temp)) {
    //        check = true
    //      }
    //      i = i + 1
    //    }
    retained.append(temp)
    if (!(temp.equals(processedName))) {
      map = seg(processedName, pdic)
    } else {
      map += ("retained" -> retained.toList)
    }

    println("map:"+map)
    println("map size:"+map.size)

    println("retained list:"+map.get("retained"))
    println("removed list:"+map.get("removed"))

    result.append(map.get("retained").last.mkString)
//    val removed = map.get("removed").last
    if(map.size==2){
      result.append(map.get("removed").last.last)
    }else{
      result.append("k")
    }

    return result.toList
  }

  def seg(processedName: String, pdic: Array[String]): HashMap[String, List[String]] = {

    val length = processedName.length
    var map = new HashMap[String, List[String]]
    val retained = new ListBuffer[String]
    val removed = new ListBuffer[String]

    var temp = new String
    var result = new String
    var i = 0
    var j = 0
    var check1 = false
    var check2 = false

    while (i < length-1 && !check1) {

      j = length
      while (j >= i && !check2) {
        temp = processedName.substring(i, j)
        if (pdic.contains(temp)) {
          removed.append(temp)
          check2 = true
        }
        if (i == j) {
          result = processedName.substring(i, length)
          check1 = true
        }
        j = j - 1
      }
      check2 = false
      j=j+1
      i = j
//      println("test i:"+i+" j:"+j)
    }

    retained.append(result)
    map += ("retained" -> retained.toList)
    map += ("removed" -> removed.toList)
    return map


    //    breakable {
    //      for (i <- 0 to length - 1) {
    //
    //        j = length
    //
    //        while (j >= i && !check2) {
    //          if (pdic.contains(temp)) {
    //            removed.append(temp)
    //            check2 = true
    //          }
    //          j = j-1
    //        }
    //        if (i == j) {
    //          result = processedName.substring(i, length)
    //          break()
    //        }
    ////        i=j
    //      }
    //    }
    //    return map
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val name = "江苏省苏州市沧浪区中天集团"
    val name2 = "江苏省苏州市沧浪区中天集团有限公司"
    val name3 = "中国移动江苏分公司"
    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/companyNames")
    val DIC = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/dic")
    val PDIC = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/placeDic").toArray()




    val result = startSeg(name3, PDIC)
    print("test")
    println("result:"+result)
    println("simplifiedName:"+result(0))
    println("nearestPlace:"+result(1))

//    println("list retained:" + map.get("retained"))
//    println("list removed:" + map.get("removed"))
    //    println("is:" + pdic.contains("北京"))

    //    println(initialProcess(name, dic))

    //    val names = rdd1.map(line => {
    //      val fileld = line.split(",")
    //      println("completedComName:"+fileld(1))
    //      (fileld(0),initialProcess(fileld(1),dic),fileld(0))
    //    })
    //
    //  names.foreach(println _)
    //    //
    //    DIC.foreach(line => {
    //      result = name.replace(line,"")
    //    })
    //    println(result)
  }

}
