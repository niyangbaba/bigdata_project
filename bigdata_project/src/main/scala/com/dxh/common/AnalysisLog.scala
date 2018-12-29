package com.dxh.common

import java.net.URLDecoder

import com.dxh.caseclass.IPRule
import com.dxh.constants.LogConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Administrator on 2018/12/21.
  * 解析log日志
  */
object AnalysisLog {

  /**
    * 将ip解析成地域信息
    *
    * @param eventLogMap
    * map 存放切分出来的信息
    * @param ipRules
    * 根据ip规则查找对应的地域信息
    */
  private def handleIP(eventLogMap: mutable.Map[String, String], ipRules: Array[IPRule]) = {
    //从Map中获取 ip
    val ip = eventLogMap(LogConstants.LOG_COLUMNS_NAME_IP)
    //获取地域信息
    val regionInfo = AnalysisIP.analysisIp(ip, ipRules)
    //存入map中去
    eventLogMap.put(LogConstants.LOG_COLUMNS_NAME_COUNTRY, regionInfo.country)
    eventLogMap.put(LogConstants.LOG_COLUMNS_NAME_PROVINCE, regionInfo.province)
    eventLogMap.put(LogConstants.LOG_COLUMNS_NAME_CITY, regionInfo.city)
  }

  /**
    *
    * @param eventLogMap
    * map 存放切分出来的信息
    * @param requestParams
    * /log.gif?en=e_pv&ver=1&pl=pc&os_n=win10&b_n=chrome
    */
  private def handleRequestParams(eventLogMap: mutable.Map[String, String], requestParams: String) = {
    //将log 根据 ? 切分
    val logs = requestParams.split("[?]")
    //判断
    if (logs.length == 2) {
      val params = logs(1) //en=e_pv&ver=1&pl=pc&os_n=win10&b_n=chrome
      val items = params.split("[&]")
      for (item <- items) {
        //en=e_pv   ver=1   os_n=win10
        val kv = item.split("[=]")
        if (kv.length == 2) {
          //对k进行解码
          eventLogMap.put(URLDecoder.decode(kv(0), "utf-8"), URLDecoder.decode(kv(1), "utf-8"))
        }
      }
    }


  }


  /**
    * 解析log
    *124.207.252.34|1539825800.145|119.23.63.188:83|/log.gif?en=e_pv&ver=1&pl=pc&os_n=win10&b_n=chrome&b_v=55.0.2883.87&
    *1539825800.145==>1539825800145
    *
    * @param logText
    * @param ipRules
    */
  def analysisLog(logText: String, ipRules: Array[IPRule]) = {

    //定义一个可变的Map 存放 键值对
    var eventLogMap: mutable.Map[String, String] = null

    //判断 logText 是否为空
    if (StringUtils.isNotBlank(logText)) {
      //对log进行切分 分为4段
      val params = logText.split("[|]")
      //判断log文件是否为4段
      if (params.length == 4) {
        eventLogMap = mutable.Map[String, String]()
        //取出ip 放入Map 中
        eventLogMap.put(LogConstants.LOG_COLUMNS_NAME_IP, params(0).trim)
        //用户访问时间  将时间1539825800.145==>1539825800145 转换
        val access_time = (params(1).toDouble * 1000).toLong.toString
        eventLogMap.put(LogConstants.lOG_COLUMNS_NAME_ACCESS_TIME, access_time)
        //将ip解析成地域信息 并存入map
        handleIP(eventLogMap, ipRules)
        //解析请求参数，并将参数加入到map中
        handleRequestParams(eventLogMap, params(3).trim)
      }
    }
    eventLogMap
  }


}
