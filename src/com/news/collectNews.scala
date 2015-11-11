package com.news
import java.io.{IOException, BufferedWriter, FileWriter, File}

import scala.collection.mutable.{LinkedHashMap, Map, ListBuffer}
import scala.io.Source
import scala.util.control.Breaks

/**
 * Created by HM on 2015/10/9.
 */
object collectNews {
  var news: Map[String, Int] = Map()
  // //record a news,news(newsPath,parentPathName)
  var newMap: LinkedHashMap[String, Int] = LinkedHashMap()
  //an ordered map sort by parentPathName
  val loop = new Breaks

  def walk(file: File) {
    if (file.isFile()) {
      news += (file.getPath() -> file.getParentFile().getName().toInt)
      //       println("file:"+file.getName())
    }
    else {
      //      println("folder："+file.getName)
      file.listFiles().foreach(walk)
    }
  }

  def printSortValues(map: Map[String, Int]) {
    map.toList sortBy (_._2) foreach {
      case (key, value) =>
        newMap += (key -> value)
    }
  }


  //simplify the original complete news
  def getSimpNews(path: String): String = {
    val newsBody = getNewsBody(path)
    //extract the news's body
    val simplifiedNews = new newsCompress().getKeyContentsByNews(newsBody).toList
    val result = simplifiedNews.mkString
    result
  }

  //get the body of the news
  def getNewsBody(path: String): String = {
    var newsBody = new String
    val line = Source.fromFile(path).getLines().toList
    if(line.size != 0){
      newsBody = line.last
    }
    return newsBody
  }

  //write all simplified news into a txt file,(newsPath::simplifiedNews::newsBody)for each row
  def buildSimplifiedNewsFile(resultFile: String, newsFile: String): Unit = {
    var testPath = new String

    var fw: FileWriter = null
    var bw: BufferedWriter = null
    try {
      fw = new FileWriter(new File(resultFile))
      bw = new BufferedWriter(fw)

      walk(new File(newsFile))
      printSortValues(news)
      newMap.foreach(e => {
        val (k, v) = e
        testPath = k
        val temp = getSimpNews(k)   //temp:simplified news
        val newsBody = getNewsBody(k)
        println(k + "::" + temp + "::" + newsBody)
        if (temp.length > 4) {
          bw.write(k + "::" + temp + "::" + newsBody)
          bw.newLine()
          bw.flush()
        }
      })

    } catch {
      case ex: IOException => {
        println("IO Exception")
      }
      case ex1: NoSuchElementException =>{
        println(testPath)
      }
    } finally {
      fw.close()
      bw.close()
    }

  }
  def main(args: Array[String]) {
    val start = System.currentTimeMillis()

    //        buildSimplifiedNewsFile("D://E/files/step2/news.txt","D:/E/files/fileZhou")
            buildSimplifiedNewsFile("D://E/files/step2/news2.txt", "D:/E/files/fileZhou2")
//    buildSimplifiedNewsFile("D://E/files/step2/news3.txt", "F:/crawl2/news/sohu.com")

    val end = System.currentTimeMillis()
    println("time consuming：" + (end - start)/1000 + "second")

  }

}
