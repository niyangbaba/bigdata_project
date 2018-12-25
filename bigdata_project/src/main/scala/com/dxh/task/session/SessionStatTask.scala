package com.dxh.task.session


import com.alibaba.fastjson.JSON
import com.dxh.bean.SparkTask
import com.dxh.constants.{GlobalConstants, LogConstants}
import com.dxh.dao.SparkTaskDao
import com.dxh.enum.EventEnum
import com.dxh.task.BaseTask
import com.dxh.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/12/24.
  */
object SessionStatTask extends BaseTask {

  var taskID: Int = 0
  var sparkTask: SparkTask = null

  /**
    * 验证输入参数是否正确
    * 1，参数的个数：至少需要一个参数
    * 2，参数的类型：必须是一个数字
    *
    * @param args
    */
  private def validateInputArgs(args: Array[String]): Unit = {

    if (args.length == 0) {
      println(
        """
          |User:com.dxh.task.session.SessionStatTask
          |INFO:至少需要一个参数
        """.stripMargin)
      System.exit(0)
    }

    if (!Utils.isNum(args(0))) {
      println(
        """
          |User:com.dxh.task.session.SessionStatTask
          |INFO:参数必须是数字
        """.stripMargin)
      System.exit(0)
    }
    taskID = args(0).toInt
  }


  /**
    * 从mysql中读取任务信息
    */
  private def loadSparkTaskFromMySql(): Unit = {
    sparkTask = SparkTaskDao.getSparkTaskByTaskID(taskID)
    //判断
    if (sparkTask == null) {
      println(
        """
          |User:com.dxh.task.session.SessionStatTask
          |INFO:找不到该taskid所对应的任务
        """.stripMargin)
      System.exit(0)
    }
  }

  /**
    * 加载 TableInputFormat org.apache.hadoop.hbase.mapreduce.TableInputFormat
    * 上传 TableOutputFormat org.apache.hadoop.hbase.mapred.TableOutputFormat
    * 从hbase中读取符合任务参数的session访问记录
    */
  private def loadDataFromHbase() = {
    //task_param={"startDate":"2018-10-18 00:00:00","endDate":"2018-10-19 00:00:00"}
    //将参数 转换为json对象
    val jSONObject = JSON.parseObject(sparkTask.task_param)
    //将参数 转换为时间戳
    val startTime = Utils.parseDate(jSONObject.getString(GlobalConstants.START_DATE), "yyyy-MM-dd HH:mm:ss")
    val endTime = Utils.parseDate(jSONObject.getString(GlobalConstants.END_DATE), "yyyy-MM-dd HH:mm:ss")

    //构建Scan 扫描仪
    val scan = new Scan()
    //设置扫描器的开始和结束位置(包前不包后)
    scan.setStartRow(startTime.toString.getBytes())
    scan.setStopRow(endTime.toString.getBytes())

    //将scan转换成字符串
    val proScan = ProtobufUtil.toScan(scan)
    val base64StringScan = Base64.encodeBytes(proScan.toByteArray)

    val jobConf = new JobConf(configuration)

    //设置需要加载数据的目标表
    jobConf.set(TableInputFormat.INPUT_TABLE, LogConstants.HBASE_LOG_TABLE)
    //设置扫描器
    jobConf.set(TableInputFormat.SCAN, base64StringScan)

    //调用Spark加载数据
    val tuple11RDD = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(tuple2 => tuple2._2) // tuple2  rowkey,value  这里只需要 value 不需要操作行键
      .map(result => {
      //列族  列
      val uuid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_UUID.getBytes()))
      val sid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_SID.getBytes()))
      val eventName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_EVENT_NAME.getBytes()))
      val accessTime = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.lOG_COLUMNS_NAME_ACCESS_TIME.getBytes()))
      val browserName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_BROWSER_NAME.getBytes()))
      val osName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes()))
      val keyword = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_KEYWORD.getBytes()))
      val gid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_GOODS_ID.getBytes()))
      val country = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_COUNTRY.getBytes()))
      val province = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_PROVINCE.getBytes()))
      val city = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_CITY.getBytes()))
      (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    })

    //返回tuple11
    tuple11RDD
  }

  /**
    *
    * @param tuple11RDD
    * *(uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
    * (uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
    * (uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
    * (uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
    * (uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
    *
    * (sid,List((uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city),(uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)))
    * (sid,List((uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city),(uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)))
    *
    * sid=1|country=中国|province=江苏|city=南京|visitTimeLength=5|visitStepLength=6|keywords=xm,hw..|gids=1,6,8,9|...
    * sid=2|country=中国|province=江苏|city=南京|visitTimeLength=5|visitStepLength=6|keywords=xm,hw..|gids=1,6,8,9|...
    *
    */
  private def sessionVisitTimeAndStepLengthStat(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {

    //根据sid进行分组
    tuple11RDD.groupBy(_._2).map(tuple2 => {
      //tuple2  sid ,list(......)

      var country, province, city: String = null
      var visitTimeLength, visitStepLength: Long = 0
      var startTime, endTime: Long = 0
      val keywordBuffer = ArrayBuffer[String]()
      val goodsBuffer = ArrayBuffer[String]()

      tuple2._2.foreach(line => {
        //uuid, sid, eventName, accessTime, osName, browserName, keyword, gid, country, province, city)
        //同一个sid 的数据 country, province, city 一致
        if (country == null) country = line._9
        if (province == null) province = line._10
        if (city == null) city = line._11

        //计算 时长
        val access_time = line._4.toLong
        if (startTime == 0 || access_time < startTime) startTime = access_time
        if (endTime == 0 || access_time > endTime) endTime = access_time

        //计算 步长
        val eventName = line._3
        if (eventName.equals(EventEnum.pageViewEvent.toString))
          visitStepLength += 1 //步长加一

        //查询 事件
        if (eventName.equals(EventEnum.searchEvent.toString) && StringUtils.isNotBlank(line._7))
          keywordBuffer.append(line._7)
        //购物车事件
        if (eventName.equals(EventEnum.addCartEvent.toString) && StringUtils.isNotBlank(line._8))
          goodsBuffer.append(line._8)

      })

      //访问时长
      visitTimeLength = (endTime - startTime) / 1000

      //sid=2|country=中国|province=江苏|city=南京|visitTimeLength=5|visitStepLength=6|keywords=xm,hw..|gids=1,6,8,9|...

      var temp = GlobalConstants.FIELD_SESSION_ID + "=" + tuple2._1 + "|" +
        GlobalConstants.FIELD_COUNTRY + "=" + country + "|" +
        GlobalConstants.FIELD_PROVINCE + "=" + province + "|" +
        GlobalConstants.FIELD_CITY + "=" + city + "|" +
        GlobalConstants.FIELD_VISIT_TIME_LENGTH + "=" + visitTimeLength + "|" +
        GlobalConstants.FIELD_VISIT_STEP_LENGTH + "=" + visitStepLength

      if (keywordBuffer.length > 0)
        temp += "|" +  GlobalConstants.FIELD_KEYWORDS + "=" + keywordBuffer.mkString(",")

      if (goodsBuffer.length > 0)
        temp += "|" + GlobalConstants.FIELD_GOODS_IDS + "=" + goodsBuffer.mkString(",")

      //返回temp
      temp
    })


  }

  def main(args: Array[String]): Unit = {

    //1，验证输入参数是否正确
    validateInputArgs(args)
    //2,从数据库中读取任务参数
    loadSparkTaskFromMySql()
    //3,从hbase中读取符合任务参数的session访问记录
    val tuple11RDD = loadDataFromHbase()
    //4,调用spark各类算子，进行session的访问时长和补偿的分析性统计，最终将结果保存到mysql中
    sessionVisitTimeAndStepLengthStat(tuple11RDD)
    sc.stop()
  }


}
