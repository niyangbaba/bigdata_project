package com.dxh.task.analysislog

import com.dxh.caseclass.IPRule
import com.dxh.common.AnalysisLog
import com.dxh.conf.ConfigurationManage
import com.dxh.constants.{GlobalConstants, LogConstants}
import com.dxh.task.BaseTask
import com.dxh.utils.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * Created by Administrator on 2018/12/22.
  */
object AnalsysisLogTask extends BaseTask {

  //变量存放日期
  var inputData: String = null
  var inputPath: String = null

  //输入记录累加器
  val inputRecordAccumulator = sc.longAccumulator("inputRecordAccumulator")
  //过滤记录累加器
  val filterRecordAccumulator = sc.longAccumulator("filterRecordAccumulator")


  def main(args: Array[String]): Unit = {

    //1.验证参数是否正确
    validateInputArgs(args)
    //2.验证当前日期是否存在对应的日志(验证路径是否存在)  HDFS的路径是否存在
    validateInputPathExist()
    //3.使用spark加载ip规则库
    val ipRules = loadIPRules()
    //4.使用spark去hdfs上读取需要解析的日志
    val eventLogMapRDD = loadLog(ipRules)
    //eventLogMapRDD.foreach(println(_))

    //5,将解析好的日志保存到hbase中
    saveLogToHbase(eventLogMapRDD)

    println(s"本次输入的记录数：${inputRecordAccumulator.value},过滤记录数：${filterRecordAccumulator.value},输出记录数：${inputRecordAccumulator.value - filterRecordAccumulator.value}")

    sc.stop()

  }

  /**
    * 将解析好的日志保存到hbase中
    *
    * @param eventLogMapRDD
    */
  private def saveLogToHbase(eventLogMapRDD: RDD[mutable.Map[String, String]]): Unit = {
    val tuple2Rdd = eventLogMapRDD.map(map => {
      //唯一，散列，长度不宜过长，方便查询
      //access_time+"_"+(uuid+eventName).hashcode

      val accessTime = map(LogConstants.lOG_COLUMNS_NAME_ACCESS_TIME)
      val uuid = map(LogConstants.LOG_COLUMNS_NAME_UUID)
      val eventName = map(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)

      //设计rowkey
      val rowKey = accessTime + "_" + Math.abs((uuid + eventName).hashCode)
      //创建一个put对象
      val put = new Put(rowKey.getBytes())
      //循环遍历每一个map  获取map的kv
      map.foreach(tuple2 => {
        put.addColumn(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), tuple2._1.getBytes(), tuple2._2.getBytes())
      })

      //返回
      (new ImmutableBytesWritable(), put)
    })

    val jobConf = new JobConf(configuration)

    //指定需要采用哪个类将结果写入到hbase中
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    //指定写入的目标表 create 'event_log','log'
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, LogConstants.HBASE_LOG_TABLE)

    tuple2Rdd.saveAsHadoopDataset(jobConf)
  }

  /**
    * 使用spark去hdfs上读取需要解析的日志 解析
    *
    * @param ipRules
    */
  private def loadLog(ipRules: Array[IPRule]) = {

    val  eventMapRdd : RDD[mutable.Map[String, String]] = sc.textFile(inputPath).map(logText => {
      inputRecordAccumulator.add(1)
      AnalysisLog.analysisLog(logText, ipRules)
    })
      .filter(map => {
        if (map == null) {
          filterRecordAccumulator.add(1)
          false
        } else {
          true
        }
      })
    eventMapRdd
  }

  /**
    * 使用spark加载ip规则库
    */
  private def loadIPRules() = {

    val iPRules = sc.textFile(ConfigurationManage.getProperty(GlobalConstants.PROJECT_RESOURCE), 2).map(line => {
      //line==>1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val fields = line.split("[|]")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    /**
      * collect:
      * 1，触发任务提交
      * 2，将结果从executor端拉回到driver端
      */

    iPRules

  }

  /**
    * 验证参数是否正确
    * 1,验证参数的个数:至少需要一个参数
    * 2，验证参数的格式 ：yyyy-MM-dd
    *
    * @param args
    */
  private def validateInputArgs(args: Array[String]) = {

    if (args.length == 0) {
      println(
        """
          |Usage:com.dxh.task.analysislog.AnalsysisLogTask
          |info:任务至少需要一个参数
        """.stripMargin)
      System.exit(0)
    }

    //对第一个参数 进行判断 日期格式必须是 yyyy-MM-dd
    if (!Utils.validateInputDate(args(0))) {
      println(
        """
          |Usage:com.dxh.task.analysislog.AnalsysisLogTask
          |info:参数格式错误
        """.stripMargin)
      System.exit(0)
    }

    inputData = args(0)
  }

  /**
    * 验证当前日期是否存在对应的日志(验证路径是否存在)
    * 2018-10-18===>2018/10/18==>/logs/2018/10/18
    */
  private def validateInputPathExist(): Unit = {

    inputPath = "/logs/" + Utils.formatDate(Utils.parseDate(inputData, "yyyy-MM-dd"), "yyyy/MM/dd")

    var fileSystem: FileSystem = null

    try {
      fileSystem = FileSystem.newInstance(configuration)

      if (!fileSystem.exists(new Path(inputPath))) {

        println(
          """
            |Usage:com.dxh.task.analysislog.AnalsysisLogTask
            |INFO:输入路径不存在
          """.stripMargin)

        System.exit(0)
      }

    } catch {

      case e: Exception => e.printStackTrace()
    } finally {

      if (fileSystem != null)
        fileSystem.close()

    }


  }


}















