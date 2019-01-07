package com.dxh.task.review

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable

/**
  * Created by Administrator on 2018/12/29.
  */
object SocketStreamingTask {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    //从spark1.5之后，引入了反压机制，可以根据sparkstreaming任务处理速率，动态的调整receiver的接收速率
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc = sparkSession.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("file:\\G:\\a")

    //    val inputDstream: ReceiverInputDStream[String] = ssc.socketTextStream("h21", 9998, StorageLevel.MEMORY_AND_DISK)

    //开启多个dstream 进行合并
    val inputDstreams: immutable.IndexedSeq[ReceiverInputDStream[String]] = (0 to 3).map(x => {
      ssc.socketTextStream("h21", 9988, StorageLevel.MEMORY_AND_DISK)
    })

    val inputDStream: DStream[String] = ssc.union(inputDstreams)

    val wordDStream: DStream[String] = inputDStream.flatMap(_.split(" "))

    val mapDStream = wordDStream.map((_, 1))

    val reduceDStream = mapDStream.reduceByKey(_ + _)

    //持久化
    //    reduceDStream.cache()
    reduceDStream.persist(StorageLevel.MEMORY_ONLY)
    //chickpoint
    reduceDStream.checkpoint(Seconds(10))


    reduceDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
