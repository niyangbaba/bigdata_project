package com.dxh.task.session


import java.sql.ResultSet

import com.alibaba.fastjson.JSON
import com.dxh.bean.{AreaTop3Product, SessionAggrStat, SparkTask, Top5Category}
import com.dxh.constants.{GlobalConstants, LogConstants}
import com.dxh.dao.{AreaTop3ProductDao, SessionAggrStatDao, SparkTaskDao, Top5CategoryDao}
import com.dxh.enum.EventEnum
import com.dxh.jdbc.JDBCHelper
import com.dxh.task.BaseTask
import com.dxh.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Accumulable
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/12/24.
  */
object SessionStatTask extends BaseTask {

  var taskID: Int = 0
  var sparkTask: SparkTask = _

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
    //设置扫描器的开始和结束位置 (包前不包后) **不清楚
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
    * 计算访问时长所属区间
    *
    * @param visitTimeLength
    * @param sessionAccumulator
    */
  private def calculateVisitTimeRange(visitTimeLength: Int, sessionAccumulator: Accumulable[String, String]): Unit = {

    if (visitTimeLength >= 0 && visitTimeLength <= 3) {
      sessionAccumulator.add(GlobalConstants.TIME_1s_3s)
    } else if (visitTimeLength >= 4 && visitTimeLength <= 6) {
      sessionAccumulator.add(GlobalConstants.TIME_4s_6s)
    } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
      sessionAccumulator.add(GlobalConstants.TIME_7s_9s)
    } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
      sessionAccumulator.add(GlobalConstants.TIME_10s_30s)
    } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
      sessionAccumulator.add(GlobalConstants.TIME_30s_60s)
    } else if (visitTimeLength > 1 * 60 && visitTimeLength <= 3 * 60) {
      sessionAccumulator.add(GlobalConstants.TIME_1m_3m)
    } else if (visitTimeLength > 3 * 60 && visitTimeLength <= 10 * 60) {
      sessionAccumulator.add(GlobalConstants.TIME_3m_10m)
    } else if (visitTimeLength > 10 * 60 && visitTimeLength <= 30 * 60) {
      sessionAccumulator.add(GlobalConstants.TIME_10m_30m)
    } else if (visitTimeLength > 30 * 60) {
      sessionAccumulator.add(GlobalConstants.TIME_30m)
    }

  }

  /**
    * 计算访问步长所属区间
    *
    * @param visitStepLength
    * @param sessionAccumulator
    */
  private def calculateVisitStepRange(visitStepLength: Int, sessionAccumulator: Accumulable[String, String]): Unit = {
    if (visitStepLength >= 1 && visitStepLength <= 3) {
      sessionAccumulator.add(GlobalConstants.STEP_1_3)
    } else if (visitStepLength >= 4 && visitStepLength <= 6) {
      sessionAccumulator.add(GlobalConstants.STEP_4_6)
    } else if (visitStepLength >= 7 && visitStepLength <= 9) {
      sessionAccumulator.add(GlobalConstants.STEP_7_9)
    } else if (visitStepLength >= 10 && visitStepLength <= 30) {
      sessionAccumulator.add(GlobalConstants.STEP_10_30)
    } else if (visitStepLength > 30 && visitStepLength <= 60) {
      sessionAccumulator.add(GlobalConstants.STEP_30_60)
    } else if (visitStepLength > 60) {
      sessionAccumulator.add(GlobalConstants.STEP_60)
    }

  }

  /**
    *
    * @param value
    */
  private def saveSessionVistTimeAndVistStepResultToMySQL(value: String) = {
    //先删除 taskid对应 的mysql上的数据
    SessionAggrStatDao.deleteByTaskId(taskID)
    //获取sessionCount
    val session_count = Utils.getFieldValue(value, GlobalConstants.SESSION_COUNT).toInt
    //将累加器的值 存入到sessionAggrStat对象中
    val sessionAggrStat: SessionAggrStat = new SessionAggrStat()
    sessionAggrStat.task_id = taskID
    sessionAggrStat.session_count = session_count
    //保留两位 小数 四舍五入    toint 值会为 0
    sessionAggrStat.time_1s_3s = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_1s_3s).toDouble / session_count, 2)
    sessionAggrStat.time_4s_6s = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_4s_6s).toDouble / session_count, 2)
    sessionAggrStat.time_7s_9s = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_7s_9s).toDouble / session_count, 2)
    sessionAggrStat.time_10s_30s = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_10s_30s).toDouble / session_count, 2)
    sessionAggrStat.time_30s_60s = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_30s_60s).toDouble / session_count, 2)
    sessionAggrStat.time_1m_3m = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_1m_3m).toDouble / session_count, 2)
    sessionAggrStat.time_3m_10m = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_3m_10m).toDouble / session_count, 2)
    sessionAggrStat.time_10m_30m = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_10m_30m).toDouble / session_count, 2)
    sessionAggrStat.time_30m = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.TIME_30m).toDouble / session_count, 2)
    sessionAggrStat.step_1_3 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_1_3).toDouble / session_count, 2)
    sessionAggrStat.step_4_6 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_4_6).toDouble / session_count, 2)
    sessionAggrStat.step_7_9 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_7_9).toDouble / session_count, 2)
    sessionAggrStat.step_10_30 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_10_30).toDouble / session_count, 2)
    sessionAggrStat.step_30_60 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_30_60).toDouble / session_count, 2)
    sessionAggrStat.step_60 = Utils.getScale(Utils.getFieldValue(value, GlobalConstants.STEP_60).toDouble / session_count, 2)

    //存入mysql
    SessionAggrStatDao.insert(sessionAggrStat)

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
    val sessionBehaviorStrRDD = tuple11RDD.groupBy(_._2).map(tuple2 => {
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

      if (keywordBuffer.nonEmpty)
        temp += "|" + GlobalConstants.FIELD_KEYWORDS + "=" + keywordBuffer.mkString(",")

      if (goodsBuffer.nonEmpty)
        temp += "|" + GlobalConstants.FIELD_GOODS_IDS + "=" + goodsBuffer.mkString(",")

      //返回temp
      temp
    })

    //自定义的一个累加器
    val sessionAccumulator = sc.accumulable("")(SessionAccumulator)

    //访问时长累加器
    val visitTimeLengthTotalAccumulator = sc.longAccumulator("visitTimeLengthTotalAccumulator")
    //访问步长累计器
    val visitStepLengthTotalAccumulator = sc.longAccumulator("visitStepLengthTotalAccumulator")

    sessionBehaviorStrRDD.foreach(line => {
      //session_count 字段进行累加
      sessionAccumulator.add(GlobalConstants.SESSION_COUNT)
      //取出访问时长
      val visitTimeLength = Utils.getFieldValue(line, GlobalConstants.FIELD_VISIT_TIME_LENGTH).toInt
      visitTimeLengthTotalAccumulator.add(visitTimeLength)
      //计算访问时长所属区间
      calculateVisitTimeRange(visitTimeLength, sessionAccumulator)

      //取出访问步长
      val visitStepLength = Utils.getFieldValue(line, GlobalConstants.FIELD_VISIT_STEP_LENGTH).toInt
      visitStepLengthTotalAccumulator.add(visitStepLength)
      //计算访问步长所属区间
      calculateVisitStepRange(visitStepLength, sessionAccumulator)

    })

    //将session访问时长和步长占比保持到mysql
    saveSessionVistTimeAndVistStepResultToMySQL(sessionAccumulator.value)

    val avgVisitTimeLength = Utils.getScale(visitTimeLengthTotalAccumulator.value.toDouble / Utils.getFieldValue(sessionAccumulator.value, GlobalConstants.SESSION_COUNT).toInt, 2)
    val avgVisitStepLength = Utils.getScale(visitStepLengthTotalAccumulator.value.toDouble / Utils.getFieldValue(sessionAccumulator.value, GlobalConstants.SESSION_COUNT).toInt, 2)

    println(s"web端平均访问时长：${avgVisitTimeLength}")
    println(s"web端平均访问深度：${avgVisitStepLength}")


  }

  /**
    * 统计每天的新增用户数
    *
    * @param tuple11RDD
    * (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    */
  private def calculateNewUser(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {

    tuple11RDD.filter(x => x._3.equals(EventEnum.launchEvent.toString))
      .map(line => {
        (Utils.formatDate(line._4.toLong, "yyyy-MM-dd"), 1)
      }).reduceByKey(_ + _)
      .foreach(println(_))

  }

  /**
    * 统计每个地区(省份)的uv(独立访客)
    *
    * @param tuple11RDD
    * (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    */
  private def calculateProvinceUV(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    tuple11RDD.map(x => (x._10, x._1)).distinct().map(x => (x._1, 1)).reduceByKey(_ + _).foreach(println(_))
  }

  /**
    * 统计每天每款浏览器的uv(独立访客)
    *
    * @param tuple11RDD
    * (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    */
  private def calculateBrowserUv(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {

    tuple11RDD.map(x => ((Utils.formatDate(x._4.toLong, "yyyy-MM-dd"), x._5), x._1)).distinct().map(x => (x._1, 1)).reduceByKey(_ + _).foreach(println(_))

  }

  /**
    * 在符合条件的session中，获取点击、加入购物车，下单，支付数量排名前5的品类
    *
    * @param tuple11RDD
    * (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    *
    */
  private def calculateCategoryTop5(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    //(gid,eventName)
    val gidEventNameRDD = tuple11RDD.filter(x => StringUtils.isNotBlank(x._8)).map(x => (x._8, x._3))
    //从mysql数据库中读取商品与品类数据
    //(gid,cat_id)
    val gidCatIdRDD = new JdbcRDD[(String, String)](
      sc,
      () => JDBCHelper.getConnection(),
      "select goods_id,cat_id from ecs_goods where goods_id>? and goods_id<?",
      0,
      Int.MaxValue,
      1,
      (resultSet: ResultSet) => {
        (resultSet.getString("goods_id"), resultSet.getString("cat_id"))
      }
    )

    // (gid,(eventName,cat_id))
    val joinRDD = gidEventNameRDD.join(gidCatIdRDD)
    //需要将商品发生的事件，转换成品类发生的事件(gid,eventName)==>(cat_id,eventName)
    val catIdEventNameRDD = joinRDD.map(x => (x._2._2, x._2._1))

    //发生过事件的品类(cat_id,cat_id)
    val categoryRDD = catIdEventNameRDD.map(x => (x._1, x._1)).distinct()
    //每个品类被点击次数的rdd (cat_id,click_count)
    val clickCountRDD = catIdEventNameRDD.filter(x => x._2.equals(EventEnum.pageViewEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //每个品类被加入购物车次数的rdd  (cat_id,cart_count)
    val cartCountRDD = catIdEventNameRDD.filter(x => x._2.equals(EventEnum.addCartEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //每个品类被下单次数的rdd (cat_id,order_count)
    val orderCountRDD = catIdEventNameRDD.filter(x => x._2.equals(EventEnum.orderEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //每个品类被支付次数的rdd (cat_id,pay_count)
    val payCountRDD = catIdEventNameRDD.filter(x => x._2.equals(EventEnum.payEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)

    val categoryBehaviorRDD = categoryRDD.leftOuterJoin(clickCountRDD) //(cat_id,(cat_id,click_count))
      .map(x => {
      var click_count = 0
      if (x._2._2.nonEmpty) {
        click_count = x._2._2.get
      }
      (x._1, GlobalConstants.CLICK_COUNT + "=" + click_count)
    }).leftOuterJoin(cartCountRDD) //(cat_id,(click=xx,cart_count))
      .map(x => {
      var cart_count = 0
      if (x._2._2.nonEmpty) {
        cart_count = x._2._2.get
      }
      (x._1, x._2._1 + "|" + GlobalConstants.CART_COUNT + "=" + cart_count)
    }).leftOuterJoin(orderCountRDD) //(cat_id,(click=xx|cart_count=xx,order_count))
      .map(x => {
      var order_count = 0
      if (x._2._2.nonEmpty) {
        order_count = x._2._2.get
      }
      (x._1, x._2._1 + "|" + GlobalConstants.ORDER_COUNT + "=" + order_count)
    }).leftOuterJoin(payCountRDD) //(cat_id,(click=xx|cart_count=xx|order_count=xx,pay_count))
      .map(x => {
      var pay_count = 0
      if (x._2._2.nonEmpty) {
        pay_count = x._2._2.get
      }
      (x._1, x._2._1 + "|" + GlobalConstants.PAY_COUNT + "=" + pay_count)
    }) //(cat_id,(click=xx|cart_count=xx|order_count=xx|pay_count=xx))

    //对计算的结果 自定义二次排序 取出前5
    val top5CategoryArray: Array[Top5Category] = categoryBehaviorRDD.map(x => {
      new Top5Category(
        taskID,
        x._1.toInt,
        Utils.getFieldValue(x._2, GlobalConstants.CLICK_COUNT).toInt,
        Utils.getFieldValue(x._2, GlobalConstants.CART_COUNT).toInt,
        Utils.getFieldValue(x._2, GlobalConstants.ORDER_COUNT).toInt,
        Utils.getFieldValue(x._2, GlobalConstants.PAY_COUNT).toInt
      )
    }).sortBy(x => x, false, 1).take(5)

    //将结果存入 mysql中去
    Top5CategoryDao.deleteByTaskId(taskID)
    Top5CategoryDao.insert(top5CategoryArray)

  }

  /**
    * 统计每个省份加入购物车排名前三的商品
    *
    * @param tuple11RDD
    * (uuid, sid, eventName, accessTime, browserName, osName, keyword, gid, country, province, city)
    */
  private def addCartTop3GoodsStat(tuple11RDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {

    /*//隐式转换
    import  spark.implicits._
    //先过滤掉 不是加入购物车的事件 同时gid 不能为空
    val provinceCityGidRDD = tuple11RDD.filter(x => x._3.equals(EventEnum.addCartEvent.toString) && StringUtils.isNotBlank(x._8))
      //做一次map 得到 (gid,province,city)
      .map(x => ProvinceCityGid(x._10, x._11, x._8))
    //第一种   通过反射的方式构建 dataSet
    provinceCityGidRDD.toDS().show()*/


    //第二种 通过元数据的方式 构建 dataSet
    val rowRdd: RDD[Row] = tuple11RDD.filter(x => x._3.equals(EventEnum.addCartEvent.toString) && StringUtils.isNotBlank(x._8))
      .map(x => Row(x._10, x._11, x._8))


    val schema = StructType(List(
      StructField("province", StringType, true),
      StructField("city", StringType, true),
      StructField("gid", StringType, true)
    ))
    import spark.sql
    //创建dateFrame 并创建临时视图
    spark.createDataFrame(rowRdd, schema).createOrReplaceTempView("province_city_gid_view")

    sql("select province,city,gid,count(gid)cart_count  from province_city_gid_view group by province,city,gid")
      .createOrReplaceTempView("province_city_gid_cart_count_view")

    //注册临时函数
    spark.udf.register("city_concat_func", new CityConcatUDAF)

    //自定义的函数 和 开窗函数 进行 聚合 组内排序

    val areaTop3ProductArray = ArrayBuffer[AreaTop3Product]()
    sql(
      """
        |select province,gid,cart_count,cities
        |from(
        |   select row_number()over( partition by  province order by cart_count desc) rank,province,gid,cart_count,cities
        |   from(
        |        select province,gid,sum(cart_count) cart_count ,city_concat_func(city) cities
        |        from  province_city_gid_cart_count_view
        |        group by   province,gid
        |   )temp
        |)tmp
        |where tmp.rank<=3
      """.stripMargin)
      .collect() //将计算结果拉回Driver 端
      .foreach(row => { //这是scala的api 不是spark的算子
      val areaTop3Product = new AreaTop3Product(
        taskID,
        row.getAs[String]("province"),
        row.getAs[String]("gid"),
        row.getAs[Long]("cart_count"), //在sql中使用 sum 结果变为 Long 类型
        row.getAs[String]("cities")
      )
      areaTop3ProductArray.append(areaTop3Product)
    })

    //将数据 存入到mysql中
    AreaTop3ProductDao.deleteByTaskId(taskID)
    AreaTop3ProductDao.insert(areaTop3ProductArray.toArray)


  }

  def main(args: Array[String]): Unit = {

    //1，验证输入参数是否正确
    validateInputArgs(args)
    //2,从数据库中读取任务参数
    loadSparkTaskFromMySql()
    //3,从hbase中读取符合任务参数的session访问记录
    val tuple11RDD = loadDataFromHbase()
    //4,调用spark各类算子，进行session的访问时长和补偿的分析性统计，最终将结果保存到mysql中
    //    sessionVisitTimeAndStepLengthStat(tuple11RDD)


    //统计每天的新增用户数
    //    calculateNewUser(tuple11RDD)

    //统计每个地区(省份)的uv(独立访客)
    //    calculateProvinceUV(tuple11RDD)

    //统计每天每款浏览器的uv(独立访客)
    //    calculateBrowserUv(tuple11RDD)


    //在符合条件的session中，获取点击、加入购物车，下单，支付数量排名前5的品类
    //    calculateCategoryTop5(tuple11RDD)


    //统计每个省份加入购物车排名前三的商品
    addCartTop3GoodsStat(tuple11RDD)

    sc.stop()
  }


}
