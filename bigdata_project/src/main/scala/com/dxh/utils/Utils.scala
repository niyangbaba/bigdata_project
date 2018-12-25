package com.dxh.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

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

    for (i <- 0 until (ips.length)) //包左不包右
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
  def formatDate(longTime:Long,pattern:String) ={
    val simpleDateFormat = new SimpleDateFormat(pattern)

    simpleDateFormat.format(new Date(longTime))

  }

  /**
    * 判断输入的参数是否属数字
    * @param num
    * @return
    */
  def isNum(num : String) ={
    val reg = "\\d+"
    Pattern.compile(reg).matcher(num).matches()
  }


}













