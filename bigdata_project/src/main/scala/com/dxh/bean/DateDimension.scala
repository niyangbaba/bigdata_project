package com.dxh.bean

import com.dxh.enum.DateTypeEnum
import com.dxh.utils.Utils

/**
  * Created by Administrator on 2019/1/4.
  */
class DateDimension(var id: Int, var year: Int, var season: Int, var month: Int, var week: Int, var day: Int, var calendar: String, var dateType: String) {

}

object  DateDimension{

  /**
    * 构建时间维度对象
    * @param inputDate 日期 yyyy-MM-dd
    */
  def  buildDateDimension(inputDate:String) = {

    val longTime = Utils.parseDate(inputDate,"yyyy-MM-dd")
    val year = Utils.getDateInfo(longTime,DateTypeEnum.year)
    val season = Utils.getDateInfo(longTime, DateTypeEnum.season)
    val month = Utils.getDateInfo(longTime, DateTypeEnum.month)
    val week = Utils.getDateInfo(longTime, DateTypeEnum.week)
    val day = Utils.getDateInfo(longTime, DateTypeEnum.day)

    new DateDimension(0,year,season,month,week,day,inputDate,DateTypeEnum.day.toString)
  }


}