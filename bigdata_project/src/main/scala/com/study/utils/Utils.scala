package com.study.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

/**
  * Created by Administrator on 2018/12/29.
  */
object Utils {

  def isDate(date :String) = {

    val reg = "((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))"

    //匹配到 返回true 否则 false
    Pattern.compile(reg).matcher(date).matches()

  }

  def parseDate(date:String) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val formatDate: Date = simpleDateFormat.parse(date)

    formatDate.getTime

  }

  def formatDate(dateTime:Long) = {
    val simpleDateFormat = new  SimpleDateFormat("yyyy/MM/dd")

    simpleDateFormat.format(dateTime)
  }


}
