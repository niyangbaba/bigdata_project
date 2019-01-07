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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * Created by Administrator on 2018/12/22.
  */
object AnalsysisLogTask extends BaseTask {

  //变量存放日期
  var inputData: String = _
  var inputPath: String = _

  //输入记录累加器
  val inputRecordAccumulator = sc.longAccumulator("inputRecordAccumulator")
  //过滤记录累加器
  val filterRecordAccumulator = sc.longAccumulator("filterRecordAccumulator")


  def main(args: Array[String]): Unit = {

    //添加项目使用jar包
//    sc.addJar("F:\\SpaceWork\\LearnProject\\bigdata_project\\target\\bigdata_project-1.0-SNAPSHOT.jar")
    //1.验证参数是否正确7
    validateInputArgs(args)
    //2.验证当前日期是否存在对应的日志(验证路径是否存在)  HDFS的路径是否存在
    validateInputPathExist()
    //3.使用spark加载ip规则库
    val ipRules = loadIPRules()

    //使用广播变量
    val ipRulesBroadcast: Broadcast[Array[IPRule]] = sc.broadcast(ipRules)
    //4.使用spark去hdfs上读取需要解析的日志
    val eventLogMapRDD: RDD[mutable.Map[String, String]] = loadLog(ipRulesBroadcast)
    // eventLogMapRDD.foreach(println(_))
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
      //循环遍历每一个map  获取map的kv  对put赋值  列族 列 值
      map.foreach(tuple2 => {
        put.addColumn(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), tuple2._1.getBytes(), tuple2._2.getBytes())
      })

      //返回  TableOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Put>
      (new ImmutableBytesWritable(), put)
    })

    val jobConf = new JobConf(configuration)

    //指定需要采用哪个类将结果写入到hbase中  org.apache.hadoop.hbase.mapred.TableOutputFormat
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    //指定写入的目标表 在hbase上创建表 create 'event_log','log'
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, LogConstants.HBASE_LOG_TABLE)

    //将结果保存至HBase
    tuple2Rdd.saveAsHadoopDataset(jobConf)
  }

  /**
    * 使用spark去hdfs上读取需要解析的日志 解析
    *
    * @param ipRulesBroadcast
    */
  private def loadLog(ipRulesBroadcast: Broadcast[Array[IPRule]]) = {

    val eventMapRdd: RDD[mutable.Map[String, String]] = sc.textFile(inputPath).map(logText => {
      //累加器 加一
      inputRecordAccumulator.add(1)
      //对日志 进行  分析 返回一个可变的 map集合  需要传入 日志和 ip规则Array[IPRule]
      AnalysisLog.analysisLog(logText, ipRulesBroadcast.value)
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

    // HDFS 上的文件储存路径  将传过来的时间进行转换
    inputPath = "/logs/" + Utils.formatDate(Utils.parseDate(inputData, "yyyy-MM-dd"), "yyyy/MM/dd")

    //使用FileSystem 其操作关于hdfs文件系统
    var fileSystem: FileSystem = null

    //fileSystem 是流 需要try catch 关闭
    try {
      fileSystem = FileSystem.newInstance(configuration)

      //判断hdfs上是否存在 该路径
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

      //最终将 fileSystem关闭
      if (fileSystem != null)
        fileSystem.close()

    }


  }


}















