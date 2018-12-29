package com.study.hbase

import java.text.SimpleDateFormat

/**
  * Created by Administrator on 2018/12/27.
  */
object dateChange {

  def main(args: Array[String]): Unit = {

    val now = "1998-11-14"

    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")

    val date = dateFormat.parse(now).getTime

    val format = new SimpleDateFormat("yyyy/mm/dd")

    val str = format.format(date)

    println(date + "/****/"+ str)

  }

}
