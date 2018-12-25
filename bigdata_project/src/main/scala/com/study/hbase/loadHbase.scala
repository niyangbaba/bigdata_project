package com.study.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Created by Administrator on 2018/12/25.
  */
object loadHbase {

  def loadDataToHbase(): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val confi = new Configuration()
    confi.addResource("hbase-site.xml")

    //读取 hdfs 上的文件 处理 然后 存入 到 hbase中



    sc.stop()
  }

  def main(args: Array[String]): Unit = {

    loadDataToHbase()

  }


}
