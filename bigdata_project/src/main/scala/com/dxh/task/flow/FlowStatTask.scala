package com.dxh.task.flow

import com.dxh.bean.{DateDimension, LocationDimension, StatsLocationFlow}
import com.dxh.caseclass.IPRule
import com.dxh.common.AnalysisLog
import com.dxh.conf.ConfigurationManage
import com.dxh.constants.{GlobalConstants, LogConstants}
import com.dxh.dao.{DimensionDao, StatsLocationFlowDao}
import com.dxh.enum.EventEnum
import com.dxh.jdbc.JDBCHelper
import com.dxh.kafka.KafkaManage
import com.dxh.utils.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by Administrator on 2019/1/3.
  */
object FlowStatTask {

  val checkPointPath = ConfigurationManage.getProperty(GlobalConstants.CHECKPOINT_PATH)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[KafkaManage]))

    val sc = new SparkContext(sparkConf)

    val iPRules = sc.textFile(ConfigurationManage.getProperty(GlobalConstants.PROJECT_RESOURCE), 2).map(line => {
      //line==>1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val fields = line.split("[|]")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    val ipRulesBroadcast: Broadcast[Array[IPRule]] = sc.broadcast(iPRules)

    val ssc = StreamingContext.getOrCreate(checkPointPath, () => createStreamingContext(sc, ipRulesBroadcast))


    ssc.start()
    ssc.awaitTermination()

  }

  private def createStreamingContext(sc: SparkContext, ipRulesBroadcast: Broadcast[Array[IPRule]]) = {

    val ssc = new StreamingContext(sc, Seconds(ConfigurationManage.getProperty(GlobalConstants.BATCH_INTERVAL).toInt))
    //设置还原点
    ssc.checkpoint(checkPointPath)

    val kafkaParams = Map[String, String](
      //kafka节点信息
      GlobalConstants.METADATA_BROKER__LIST -> ConfigurationManage.getProperty(GlobalConstants.METADATA_BROKER__LIST),
      //消费者组
      GlobalConstants.GROUP_ID -> ConfigurationManage.getProperty(GlobalConstants.GROUP_ID),
      //消费起始位置
      GlobalConstants.AUTO_OFFSET_RESET -> ConfigurationManage.getProperty(GlobalConstants.AUTO_OFFSET_RESET)
    )

    val kafkaManage = new KafkaManage(kafkaParams, Set(ConfigurationManage.getProperty(GlobalConstants.TOPICS)))

    val dStream: DStream[String] = kafkaManage.createDirectDStream(ssc)

    dStream.map(logText => {
      AnalysisLog.analysisLog(logText, ipRulesBroadcast.value)
    }).filter(map => map != null)
      .map(m => {
        //yyyy-MM-dd
        val accessTime = Utils.formatDate(m(LogConstants.lOG_COLUMNS_NAME_ACCESS_TIME).toLong, "yyyy-MM-dd")
        val country = m(LogConstants.LOG_COLUMNS_NAME_COUNTRY)
        val province = m(LogConstants.LOG_COLUMNS_NAME_PROVINCE)
        val city = m(LogConstants.LOG_COLUMNS_NAME_CITY)
        val eventName = m(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
        val uuid = m(LogConstants.LOG_COLUMNS_NAME_UUID)
        val sid = m(LogConstants.LOG_COLUMNS_NAME_SID)
        (accessTime, country, province, city, eventName, uuid, sid)
      })
      .flatMap(t7 => {
        Array(
          ((t7._1, t7._2, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), (t7._5, t7._6, t7._7)),
          ((t7._1, t7._2, t7._3, GlobalConstants.VALUE_OF_ALL), (t7._5, t7._6, t7._7)),
          ((t7._1, t7._2, t7._3, t7._4), (t7._5, t7._6, t7._7))
        )
      })
      .updateStateByKey(
        (it: Iterator[((String, String, String, String), Seq[(String, String, String)], Option[Array[Any]])]) => {
          it.map(t3 => {
            val array = t3._3.getOrElse(Array(0, mutable.Set[String](), 0, mutable.Map[String, Int]()))

            var nu = array(0).asInstanceOf[Int]
            val uuidSet = array(1).asInstanceOf[mutable.Set[String]]
            var pv = array(2).asInstanceOf[Int]
            val sidCountMap = array(3).asInstanceOf[mutable.Map[String, Int]]

            for (elem <- t3._2) {
              val eventName = elem._1
              val uuid = elem._2
              val sid = elem._3
              if (eventName.equals(EventEnum.launchEvent.toString)) nu += 1
              uuidSet.add(uuid)
              if (eventName.endsWith(EventEnum.pageViewEvent.toString)) pv += 1
              val sidCount = sidCountMap.getOrElse(sid, 0)
              sidCountMap.put(sid, sidCount)
            }

            array(0) = nu
            array(1) = uuidSet
            array(2) = pv
            array(3) = sidCountMap

            (t3._1, array)
          })
        },
        new HashPartitioner(sc.defaultParallelism),
        true: Boolean
      )
      .foreachRDD(rdd => {

        //不能再这里创建连接对象，在这里穿件连接对象是一个外部变量，外部变量是需要进行序列化，但是连接对象都是不能被序列化的
        //val connection= JdbcHelper.getConnection()


        if (!rdd.isEmpty()) {
          rdd.foreachPartition(partitionIt => {

            val connection = JDBCHelper.getConnection()
            val statsLocationFlowBuffer = ArrayBuffer[StatsLocationFlow]()

            partitionIt.foreach(t2 => {
              //t2==>((accessTime,country,province ,city),Array[Any])

              val array = t2._2
              val sidCountMap = array(3).asInstanceOf[mutable.Map[String, Int]]


              //accessTime==>yyyy-MM-dd
              val dateDimension = DateDimension.buildDateDimension(t2._1._1)
              //时间维度id
              val date_dimension_id = DimensionDao.getDimensionId(dateDimension, connection)

              val locationDimension = new LocationDimension(0, t2._1._2, t2._1._3, t2._1._4)
              //地域维度id
              val location_dimension_id = DimensionDao.getDimensionId(locationDimension, connection)

              val nu = array(0).asInstanceOf[Int]
              val uv = array(1).asInstanceOf[mutable.Set[String]].size
              val pv = array(2).asInstanceOf[Int]
              val sn = sidCountMap.size
//              val  on = sidCountMap.filter(t2 => t2._2 == 1 ).size
              val on = sidCountMap.count(t2 => t2._2 == 1)

              statsLocationFlowBuffer += new StatsLocationFlow(date_dimension_id, location_dimension_id, nu, uv, pv, sn, on, t2._1._1)
            })

            //将结果保存到mysql
            if (statsLocationFlowBuffer.nonEmpty) StatsLocationFlowDao.update(statsLocationFlowBuffer.toArray)
            //关闭对象
            if (connection != null) connection.close()

          })
          //更新kafka的偏移量
          kafkaManage.updateConsumerOffset()
        }
      })

    ssc
  }

}
