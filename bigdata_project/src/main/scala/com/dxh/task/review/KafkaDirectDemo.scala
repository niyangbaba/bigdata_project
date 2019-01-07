package com.dxh.task.review

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/1/2.
  */
object KafkaDirectDemo {

  val checkPointPath = ""


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)


    /* val ssc = new StreamingContext(sc,Seconds(2))

     val kafkaParams = Map[String,String](
       "metadata.broker.list" -> "h21:9092,h22:9092,h23:9092",
       "group.id" -> "NiKou",
       "auto.offset.reset" -> "smallest"

     )

     KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("event_log"))
         .map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
         */

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPointPath, () => getStreamingContext(sc))

    ssc.start()
    ssc.awaitTermination()

  }


  def getStreamingContext(sc: SparkContext) = {

    val ssc = new StreamingContext(sc, Seconds(2))
    //设置还原点目录
    ssc.checkpoint(checkPointPath)


    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "h21:9092,h22:9092,h23:9092",
      "group.id" -> "NiKou",
      "auto.offset.reset" -> "smallest"
    )

    val inputStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder
      ](ssc, kafkaParams, Set[String]("event_log"))

    inputStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc
  }


}
