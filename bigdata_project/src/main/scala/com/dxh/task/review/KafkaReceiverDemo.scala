package com.dxh.task.review

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/1/2.
  */
object KafkaReceiverDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(2))

    //    KafkaUtils.createStream(ssc, "h21:2181,h22:2181,h23:2181", "dxhGroup", Map("event_log" -> 3))
    //      .map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "h21:2181,h22:2181,h23:2181",
      //group 是为了 使数据不被多次消费
      "group.id" -> "dxhGroup",
      //在程序找不到zookeeper的对应group组内的偏移量时 启用该配置
      "auto.offset.reset" -> "largest",
      //没有设置时 默认为一分钟 更新一次偏移量
      "auto.commit.interval.ms" -> "5000"
    )


    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map("event_log" -> 3), StorageLevel.MEMORY_AND_DISK)
      .map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
