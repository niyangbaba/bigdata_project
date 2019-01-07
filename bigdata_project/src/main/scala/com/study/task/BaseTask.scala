package com.study.task

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/12/29.
  */
trait BaseTask {

  private val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

  //kryo 序列化 需要对类进行注册
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //  sparkConf.registerKryoClasses(Array(classOf[]))

  //调整sparksql在shuffle阶段的并行度
  //sparkConf.set("spark.sql.shuffle.partitions","2")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val sc = sparkSession.sparkContext

  val conf = new Configuration()
  conf.addResource("hbase-site.xml")

}
