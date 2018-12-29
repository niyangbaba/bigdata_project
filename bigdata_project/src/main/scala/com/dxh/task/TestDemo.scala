package com.dxh.task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2018/12/28.
  */
object TestDemo extends BaseTask {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    // 持久化

    /*val arrRDD : RDD[Int] = sc.parallelize(Array(1))

    val cacheRDD: RDD[Long] = arrRDD.map(line => {
      line + System.currentTimeMillis()
    }).cache()

    cacheRDD.foreach(println(_))
    println("======")
    cacheRDD.foreach(println(_))*/

    //重分区

    val arrRDD   = sc.parallelize(Array(1,2,3,4,5,6))

    arrRDD.repartition(5)

  }

}

class MyPartitioner extends Partitioner{
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = {

    if (key.hashCode() % 2 == 1){
      0
    } else {
      1
    }

  }
}



