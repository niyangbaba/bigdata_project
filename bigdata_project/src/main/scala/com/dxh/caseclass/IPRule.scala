package com.dxh.caseclass

import com.dxh.constants.GlobalConstants

/**
  * Created by Administrator on 2018/12/21.
  */

/**
  * ip规则
  * @param startIp
  * @param endIp
  * @param country
  * @param province
  * @param city
  */
case class IPRule(var startIp:Long,var endIp : Long,var country:String,var province:String,var city:String)

/**
  * 地域信息   赋默认值 unknown
  * @param country
  * @param province
  * @param city
  */

case class RegionInfo(var country: String=GlobalConstants.DEFAULT_VALUE, var province: String=GlobalConstants.DEFAULT_VALUE, var city: String=GlobalConstants.DEFAULT_VALUE)