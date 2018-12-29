package com.study.hbase

import org.apache.spark.SparkConf


/**
  * Created by Administrator on 2018/12/25.
  */
object loadHbase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")


  }

}
