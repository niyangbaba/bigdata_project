package com.dxh.common

import com.dxh.caseclass.{IPRule, RegionInfo}
import com.dxh.utils.Utils

import scala.util.control.Breaks._

/**
  * Created by Administrator on 2018/12/21.
  */
object AnalysisIP {

  /**
    * 将ip解析成地域信息   调用二分查找法
    * @param ip
    * @param iPRules
    * @return
    */
  def analysisIp(ip:String,iPRules : Array[IPRule]) = {
    val regionInfo = RegionInfo()

    //将ip解析成数字Long  192.168.233.21 -》198456456485
    val numIp = Utils.ip2Long(ip)
    //通过二分查找法，查找出ip对应的地域信息
    val index: Int = binnarySearch(numIp,iPRules)
    //判断返回的角标是否存在
    if(index != -1){
      val ipRule: IPRule = iPRules(index)

      regionInfo.country  = ipRule.country
      regionInfo.province = ipRule.province
      regionInfo.city = ipRule.city

    }
    //返回地域信息  国家 省份 城市
    regionInfo
  }

  /**
    * 二分查找法
    * @param ip
    * @param iPRules
    * @return
    */
 private def binnarySearch(ip:Long,iPRules : Array[IPRule]) = {
    var index = -1

    var startIndex = 0
    var endIndex = iPRules.length - 1

    breakable({
      while(startIndex <= endIndex) {
        val midIndex = (startIndex + endIndex) / 2
        val iPRule = iPRules(midIndex)
        if (ip >= iPRule.startIp && ip <= iPRule.endIp){
          index = midIndex
          break()
        }else if(ip < iPRule.startIp)
          endIndex = midIndex - 1
         else if(ip > iPRule.endIp )
          startIndex = midIndex + 1
      }
    })

    index
  }





}
