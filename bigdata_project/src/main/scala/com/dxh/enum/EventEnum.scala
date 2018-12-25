package com.dxh.enum

/**
  * Created by Administrator on 2018/12/25.
  */
object EventEnum extends Enumeration {
  //首次访问事件
  val launchEvent = Value(0, "e_l")
  //页面浏览事件
  val pageViewEvent = Value(1, "e_pv")
  //搜索事件
  val searchEvent = Value(2, "e_s")
  //加入购物车事件
  val addCartEvent = Value(3, "e_ad")
}
