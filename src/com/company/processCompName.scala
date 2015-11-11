package com.company

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Created by HM on 2015/11/3.
 */
object processCompName {

  //process the company's name according the DIC initially
  def initialProcess(completeName: String, DIC: Array[String]): String = {
    var result = new String
    breakable {
      for (line <- DIC) {
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

  //    judge whether the company's name is start with place-name,if so,turn to seg(),if not,break the process then
  //   retain the complete name to the List.
  def startSeg(processedName: String, PDIC: Array[String]): List[String] = {
    val result = new ListBuffer[String]     //the result list(simplifiedName,nearestPlaceName)/(simplifiedName,"k")
    var map = new HashMap[String, List[String]]
    val retained = new ListBuffer[String]
    val removed = new ListBuffer[String]

    var temp = new String

    breakable {
      for (i <- 0.to(processedName.length)) {
        temp = processedName.substring(0, i)
        if (PDIC.contains(temp)) {
          break()
        }
      }
    }
    retained.append(temp)
    if (!(temp.equals(processedName))) {
      map = seg(processedName, PDIC)
    } else {
      map += ("retained" -> retained.toList)
    }

    result.append(map.get("retained").last.mkString)
    if(map.size==2){
      result.append(map.get("removed").last.last)
    }else{
      result.append("k")
    }

    return result.toList
  }
  //segment the processed company's name，retain these place-name that is inside the company's name ,meanwhile get the removed list and the retained list
  def seg(processedName: String, PDIC: Array[String]): HashMap[String, List[String]] = {

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

    while (i < length - 1 && !check1) {     //cut the processedName according pdic,
      j = length
      while (j >= i && !check2) {
        temp = processedName.substring(i, j)
        if (PDIC.contains(temp)) {
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
      j = j + 1
      i = j
    }
    retained.append(result)
    map += ("retained" -> retained.toList)
    map += ("removed" -> removed.toList)
    return map
  }

  //make sure the name is not covered by any other name. If it is covered, then we put the nearest removed name into the company name.
  def checkNameWithOtherNames(splitName:RDD[(String,String,String,String)],filed:String):String ={
    var result = filed
    splitName.foreach(f =>{
      if(f._3.contains(filed) && !f._3.equals(filed)){
        result = filed + f._4
      }
    })
    return result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("processName").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/companyNames")
    val DIC = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/dic").collect()   //dictionary used for process company's name originally
    val PDIC = sc.textFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/placeDic").collect() //dictionary userd for cutting the processed name

    val names = rdd1.map(line => {
      val filed = line.split(",")
      if (filed.length == 2) {
        (filed(0), initialProcess(filed(1), DIC)) //(id,completeName)
      } else {
        ("k", "k")
      }
    }).filter(f => !f._1.equals("k"))

    val splitedName = names.map(filed => {
      val result = startSeg(filed._2,PDIC) //result(simplifiedName,nearestPlaceName)/(simplifiedName,"k")
      (filed._1, filed._2,result(0),result(1))
    })

    splitedName.foreach(f => println(f))

//    val name = splitedName.map(filed =>{
//      (filed._1,checkNameWithOtherNames(splitedName,filed._3))
//    })
//
//    name.foreach(f => println(f))

//    name.saveAsTextFile("hdfs://192.168.131.192:9000/user/humeng/project1/compNames/namesResult")


    sc.stop()
  }

}
