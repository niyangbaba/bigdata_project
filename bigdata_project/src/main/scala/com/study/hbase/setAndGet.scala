package com.study.hbase
import scala.util.control.Breaks._

/**
  * Created by Administrator on 2018/12/26.
  */
object setAndGet {
  def main(args: Array[String]): Unit = {
    println(setFieldValue("session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0","session_count","9"))
  }

  /**
    * session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0
    *
    * @param line
    * @param fieldName
    */
  def getFieldValue(line: String, fieldName: String) = {

    var fieldValue: String = null

    val startIndex = line.indexOf(fieldName)

    var endIndex = line.indexOf("|", startIndex)

    if (endIndex == -1) endIndex = line.length

    fieldValue = line.substring(startIndex + fieldName.length + 1, endIndex)

    fieldValue

  }

  def setFieldValue(line: String, fieldName: String, fieldNameNewValue: String) = {

  }
}
