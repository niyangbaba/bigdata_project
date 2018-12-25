package com.dxh.test.Test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/12/22.
  */
object fileExists {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    val configuration = new Configuration()

    val fs = FileSystem.get(configuration)


  }

}
