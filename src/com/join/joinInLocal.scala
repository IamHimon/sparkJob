package com.join

import java.io.{FileWriter, File, IOException, BufferedWriter}
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by HM on 2015/10/9.
 */
object joinInLocal {

  var fw: FileWriter = null
  var writer: BufferedWriter = null

  //build man-read result file
  def joinForMan(): Unit ={
    val writerList = ListBuffer[String]()
    try {
      val news = Source.fromFile("D:\\E\\files\\step2\\news.txt").getLines().toList
      val names = Source.fromFile("D:\\E\\files\\step2\\lastResult.txt").getLines().toList

      for (l <- names) {
        val temp1 = l.split(",")(2) //temp1:simplified name
        for (s <- news) {
          val temp2 = s.split("::")(1) //temp2:simplified news
          if (temp2.contains(temp1)) {
            val newsBody = s.split("::")(2)
            println("simplifiedName="+ temp1+"    simplifiedNews="+temp2+"    completeNews="+newsBody)
            println("write=" + s.split("::")(0) + "," + newsBody)
            writerList.append(s.split("::")(0) + "," + newsBody)
          }
        }
        if (!writerList.isEmpty) {
          println("size:"+writerList.size)
          val path = "D:\\E\\files\\step2\\join\\" + l.split(",")(0) + "," + l.split(",")(1) + ".txt"
          fw = new FileWriter(new File(path))
          writer = new BufferedWriter(fw)
          for(l <- writerList){
            println(l)
            writer.write(l)
            writer.newLine()
          }
          writerList.clear()
          writer.flush()
        }
      }
    } catch {
      case ex: IOException => {
        println("IO Exception")
      }
    } finally {
      fw.close()
      writer.close()
    }


  }

  //build machine-read result file
  def joinForMachine(resultPath:String): Unit ={
    fw = new FileWriter(resultPath)
    writer = new BufferedWriter(fw)

    try {
      val news = Source.fromFile("D:\\E\\files\\step2\\news.txt").getLines().toList
      val names = Source.fromFile("D:\\E\\files\\step2\\lastResult.txt").getLines().toList

      for (l <- names) {
        val temp1 = l.split(",")(2) //temp1:simplified name
        val companyID = l.split(",")(0)
        for (s <- news) {
          val temp2 = s.split("::")(1) //temp2:simplified news
          val newsPath = s.split("::")(0)
          if (temp2.contains(temp1)) {
            println(companyID+","+newsPath)
            writer.write(companyID+","+newsPath)
            writer.newLine()
            writer.flush()
          }
        }
      }
    } catch {
      case ex: IOException => {
        println("IO Exception")
      }
    } finally {
      fw.close()
      writer.close()
    }
  }

  def main(args: Array[String]) {

    val start = System.currentTimeMillis()
    //    joinForMan()
    joinForMachine("D:\\E\\files\\step2\\machineReadResult.txt")
    val end = System.currentTimeMillis()
    println("time consuming："+((end-start)/1000).toDouble+"second")

  }

}
