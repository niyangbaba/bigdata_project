package com.dxh.kafka

import java.util.concurrent.atomic.AtomicReference

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.collection.mutable

/**
  * Created by Administrator on 2019/1/2.
  * 偏移量由kafka管理
  * 管理消费偏移量，创建输入的DStream
  */
class KafkaManage(val kafkaParams: Map[String, String], val topics: Set[String]) extends Serializable{

  //获取kafkaCluster客户端对象
  val kafkaCluster = new KafkaCluster(kafkaParams)

  setOrUpdateOffset()

  /**
    * 设置或更新消费偏移量
    * 1，首次消费需要进行设置消费偏移量
    * 2，非首次消费需要检查消费偏移量，过时了需要进行更新
    */
  private def setOrUpdateOffset() = {
    //是否消费过的标记，默认true
    var isConsume = true

    //获取topic分区元数据信息
    val topicAndPartitions: Set[TopicAndPartition] = getPartitions()

    //尝试获取消费偏移量
    val errOrConsumerOffsets: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(kafkaParams("group.id"), topicAndPartitions)

    //判断偏移量是否存在
    if (errOrConsumerOffsets.isLeft) {
      //偏移量不存在
      isConsume = false
    }


    val offsetMap = mutable.Map[TopicAndPartition, Long]()
    if (isConsume) {
      //非首次消费 -> 非首次消费需要检查消费偏移量，过时了需要进行更新

      //取出每个topic的每个分区的消费偏移量
      val consumeMap: Map[TopicAndPartition, Long] = errOrConsumerOffsets.right.get

      //取出每个topic的每个分区最早消息的偏移量
      val earliestLeaderOffsetsMap: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = getEarliestLeaderOffsets(topicAndPartitions)


      consumeMap.foreach(t2 => {
        //取出当前分区
        val topicAndPartition: TopicAndPartition = t2._1
        //取出当前分区的消费偏移量
        val consumeOffset: Long = t2._2
        //取出当前分区最早消息偏移量
        val earliestOffset: Long = earliestLeaderOffsetsMap(topicAndPartition).offset

        if (consumeOffset < earliestOffset) {
          //消费偏移量已经过时了 需要对偏移量重新赋值
          offsetMap.put(topicAndPartition, earliestOffset)

        }
      })


    }
    else {
      // 首次消费 -> 首次消费需要进行设置消费偏移量
      val offsetReset = kafkaParams.getOrElse("auto.offset.reset", "largest")

      if (offsetReset.equals("smallest")) {
        val earliestLeaderOffsetsMap: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = getEarliestLeaderOffsets(topicAndPartitions)

        earliestLeaderOffsetsMap.foreach(t2 => {

          val topicAndPartition = t2._1
          val offset = t2._2.offset
          offsetMap.put(topicAndPartition, offset)

        })
      } else {

        val latestLeaderOffSets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = getLatestLeaderOffsets(topicAndPartitions)

        latestLeaderOffSets.foreach(t2 => {

          val topicAndPartition = t2._1
          val offset = t2._2.offset
          offsetMap.put(topicAndPartition, offset)

        })
      }
    }

    //设置或更新消费偏移量到第三方上
    kafkaCluster.setConsumerOffsets(kafkaParams("group.id"), offsetMap.toMap)

  }

  /**
    * 获取每个topic的分区信息
    */
  private def getPartitions() = {
    //尝试获取topic分区元数据信息
    val errOrTopicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topics)
    //判断是否获取到分区元数据信息
    if (errOrTopicAndPartitions.isLeft) {
      throw new SparkException("尝试获取分区元数据信息失败")
    }
    //取出分区元数据信息
    errOrTopicAndPartitions.right.get
  }

  /**
    * 取出每个topic的每个分区最早消息的偏移量
    */
  private def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {
    //尝试获取每个topic的每个分区最早消息的偏移量
    val errOrMap: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    //判断是否获取到了每个topic的每个分区最早消息的偏移量
    if (errOrMap.isLeft) {
      throw new SparkException("尝试获取每个topic的每个分区最早消息的偏移量失败")
    }
    //取出每个topic的每个分区最早消息的偏移量
    errOrMap.right.get
  }

  /**
    * 取出每个topic的每个分区最新消息的偏移量
    */
  private def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {

    val errOrMap: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)

    if (errOrMap.isLeft) {
      throw new SparkException("尝试获取每个topic的每个分区最新消息的偏移量失败")
    }

    errOrMap.right.get
  }

  /**
    * 取出起始消费消息位置的偏移量
    */
  private def getConsumeOffset() = {
    //获取分区元数据
    val topicAndPartitions: Set[TopicAndPartition] = getPartitions()
    //尝试获取消费偏移量
    val errOrMap: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(kafkaParams("group.id"), topicAndPartitions)

    if (errOrMap.isLeft) {
      throw new SparkException("尝试获取每个topic的每个分区的偏移量失败")
    }

    errOrMap.right.get
  }

  /**
    * 打印起始消费位置
    */
  private def printOffset(consumeOffsetMap: Map[TopicAndPartition, Long]): Unit = {

    println("************************************************************")

    consumeOffsetMap.foreach(t2 => {
      println(s"【topic:${t2._1.topic}   partition:${t2._1.partition}    fromOffset:${t2._2}】")
    })

    println("************************************************************")

  }

  //import java.util.concurrent.atomic.AtomicReference  concurrent包下都是适合高并发的类
  val offsetArray = new AtomicReference[Array[OffsetRange]]()

  /**
    * 创建输入的DStream，从topic中消费数据
    */
  def createDirectDStream(ssc: StreamingContext) = {

    val fromOffsets: Map[TopicAndPartition, Long] = getConsumeOffset()

    printOffset(fromOffsets)

    val inputDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc, kafkaParams, fromOffsets,
      (messageAndMetadata: MessageAndMetadata[String, String]) => messageAndMetadata.message())

    val inputDStream2= inputDStream.transform(rdd => {

      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges

      offsetArray.set(ranges)

      rdd
    })

    inputDStream2

  }

  /**
    * 更新消费偏移量
    */
   def updateConsumerOffset() = {
    //取出rdd的消费偏移量
    val ranges: Array[OffsetRange] = offsetArray.get()
    println("************************************************************")
    ranges.foreach(offsetRange => {
      val offsetMap = mutable.Map[TopicAndPartition, Long]()
      //分区
      val topicAndPartition = offsetRange.topicAndPartition()
      //结束位置偏移量
      val untilOffset = offsetRange.untilOffset
      offsetMap.put(topicAndPartition, untilOffset)
      kafkaCluster.setConsumerOffsets(kafkaParams("group.id"), offsetMap.toMap)
      println(s"正在更新消费偏移量:【topic:${offsetRange.topic}   partition:${offsetRange.partition}  fromOffset:${offsetRange.fromOffset}  untilOffset:${offsetRange.untilOffset}】")
    })
    println("************************************************************")
  }
}
