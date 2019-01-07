package com.study.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.collection.mutable

/**
  * Created by Administrator on 2019/1/3.
  */
object KafkaDeno {
  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, String](

      "metadata.broker.list" -> "h21:9092,h22:9092,h23:9092",
      "group.id" -> "dxhGroup",
      "auto.offset.reset" -> "smallest" )

    val kafkaCluster = new KafkaCluster(kafkaParams)

    val errOrMap: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set("event_log"))

    println("************************************")

    println("TopicAndPartitions"+errOrMap.right.get)

    println("************************************")

    val offset = kafkaCluster.getConsumerOffsets("dxhGroup",errOrMap.right.get)
    println(offset.right.get)


    println("************************************")
    val offsetMap = mutable.Map[TopicAndPartition, Long]()
    kafkaCluster.setConsumerOffsets("dxhGroup",offsetMap.toMap)
    val offset1 = kafkaCluster.getConsumerOffsets("dxhGroup",errOrMap.right.get)
    println(offset1.right.get)

    println("************************************")

    val lateOff = kafkaCluster.getLatestLeaderOffsets(errOrMap.right.get).right.get
    val earOff = kafkaCluster.getEarliestLeaderOffsets(errOrMap.right.get)
    println(" 最新" + lateOff +"\n"+ "最早 " + earOff)

    println("************************************")
    val noOff: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets("NiKou",errOrMap.right.get)




  }
}
