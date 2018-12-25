package com.dxh.task

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 定义一个基类 特质
  */
trait BaseTask {

  /**
    * SparkSession是spark2.0出现的一个编程切入点，它封装了了两大对象：SparkConf，SparkContext对象
    * 整合了SQLContext与HiveContext两大对象。这么做的目的是为了简化spark的使用，同时降低spark的学习难度
    */

  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

  val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")

//  configuration.set("hbase.zookeeper.quorum","h21,h22,h23")
//  configuration.set("hbase.zookeeper.property.clientPort","2181")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  //获取SparkContext 对象
  val sc = spark.sparkContext



}
