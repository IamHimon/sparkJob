package com.news

import scala.io.Source
import scala.collection.mutable._
import scala.util.matching._


/**
 * Created by HM on 2015/10/9.
 */
class newsCompress {
  /**
   * 提取文件的关键内容
   **/
  def getKeyContentsByNews(news: String ): ListBuffer[String] = {
    var subContents: ListBuffer[String] = ListBuffer()
    var keywordsList: ListBuffer[String] = ListBuffer()
    for (line <- Source.fromFile("sourceFiles/keywordset.txt", "UTF-8").getLines) {
      keywordsList += line
    }

    for (i <- 0 to keywordsList.size - 1) {
      if (news.indexOf(keywordsList(i)) != -1) {
        var tempString = ""
        val regex = new Regex("(，|。)(.*?)" + keywordsList(i))
        for (matchsting <- regex.findAllIn(news)) {
          if (matchsting.lastIndexOf("，") != -1) {
            tempString = matchsting.substring(matchsting.lastIndexOf("，") + 1, matchsting.indexOf(keywordsList(i)) + keywordsList(i).length)
          }
          else if (matchsting.lastIndexOf("。") != -1) {
            tempString = matchsting.substring(matchsting.lastIndexOf("。") + 1, matchsting.indexOf(keywordsList(i)) + keywordsList(i).length)
          }
          subContents += tempString
        }
      }
    }
    return subContents

  }


}
