package com.dxh.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern
import java.math.{BigDecimal, RoundingMode}

import scala.util.control.Breaks._


/**
  * Created by Administrator on 2018/12/21.
  */
object Utils {

  /**
    * 将ip转换为long类型 192.168.233.101
    *
    * @param ip
    */
  def ip2Long(ip: String) = {
    val ips = ip.split("[.]")

    var numIp = 0L

    for (i <- 0 until ips.length) //包左不包右
    //numIp左移8位 和 ip进行 或运算
      numIp = (numIp << 8) | ips(i).toLong

    numIp
  }

  /**
    * 验证输入的日期是否是制定的格式
    *
    * @param inputDate
    * @return
    */
  def validateInputDate(inputDate: String) = {
    val reg = "((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))"

    //匹配器    编译器      去匹配日期          是否匹配到   返回 true/false
    Pattern.compile(reg).matcher(inputDate).matches()
  }

  /**
    * 将指定格式的日期转换成时间戳
    * 2018-10-18===>Long时间戳
    */
  def parseDate(inputDate: String, pattern: String) = {
    val simpledateformat = new SimpleDateFormat(pattern)
    val date = simpledateformat.parse(inputDate)
    date.getTime
  }

  /**
    * 将时间戳转换成指定格式的日期
    * 1545580800000==>2018/12/24
    *
    * @param longTime 1545580800000
    * @param pattern  yyyy/MM/dd
    */
  def formatDate(longTime: Long, pattern: String) = {
    val simpleDateFormat = new SimpleDateFormat(pattern)

    simpleDateFormat.format(new Date(longTime))

  }

  /**
    * 判断输入的参数是否属数字
    *
    * @param num
    * @return
    */
  def isNum(num: String) = {
    val reg = "\\d+"
    Pattern.compile(reg).matcher(num).matches()
  }

  /**
    * 获取值
    * nadou!!!!!!!
    *
    * session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0
    *
    * session_count 1s_3s .
    **/
  /**
    * def getFieldValue(line: String, fieldName: String) = {
    * *
    * var fileNameValue: String = null
    * *
    * val startIndex = line.indexOf(fieldName)
    * *
    * var endIndex = line.indexOf("|", startIndex)
    * *
    * if (endIndex == -1) endIndex = line.length
    * *
    * fileNameValue = line.substring(startIndex + fieldName.length +1 , endIndex)
    * *
    * fileNameValue
    * }
    */

  /**
    * 获取字符串中每个字段的值
    *
    * @param line
    * session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=1|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param fieldName
    * session_count
    */
  def getFieldValue(line: String, fieldName: String) = {

    var fieldValue: String = null

    val items = line.split("[|]")

    breakable({
      for (i <- 0 until (items.length)) {
        val kv = items(i).split("[=]")
        if (kv(0).equals(fieldName)) {
          fieldValue = kv(1)
          break()
        }
      }
    })
    fieldValue
  }


  /**
    * 赋值
    *
    * @param line
    * session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0
    * @param fieldName
    * session_count 1s_3s .
    * @param fieldNameNewValue
    * session_count=5|1s_3s=2|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0
    */
  def setFieldValue(line: String, fieldName: String, fieldNameNewValue: String) = {

    val items = line.split("[|]")
    for (i <- 0 until (items.length)) {
      val kv = items(i).split("[=]")
      if (kv(0).equals(fieldName)) {
        kv(1) = fieldNameNewValue
      }
      items(i) = kv(0) + "=" + kv(1)
    }
    items.mkString("|")
  }

  /**
    * 将传入的double 四舍五入
    *
    * @param doubleValue 传入的值
    * @param scale       保留几位小数
    */
  def getScale(doubleValue: Double, scale: Int) = {
    val bigDecimal = new BigDecimal(doubleValue) //需要手动导入  import java.math.BigDecimal
    bigDecimal.setScale(scale, RoundingMode.HALF_UP).doubleValue()
  }

}













