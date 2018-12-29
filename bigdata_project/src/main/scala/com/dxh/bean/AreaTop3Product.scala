package com.dxh.bean

/**
  * Created by Administrator on 2018/12/28.
  */
class AreaTop3Product(
                       var task_id: Int,
                       var province: String,
                       var product_id: String,
                     //进行聚合函数的时候 会是long类型
                       var cart_count: Long,
                       var city_infos: String
                     ) {

}
