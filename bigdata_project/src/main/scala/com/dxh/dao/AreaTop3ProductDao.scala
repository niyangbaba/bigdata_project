package com.dxh.dao

import com.dxh.bean.AreaTop3Product
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2018/12/28.
  */
object AreaTop3ProductDao {


  /**
    *
    * @param taskId
    * 删除taskid对应的mysql上 的任务数据
    */
  def deleteByTaskId(taskId: Int) = {
    val sql = "delete from area_top3_product where task_id=?"
    val sqlParams = Array[Any](taskId)
    JDBCHelper.executeUpdate(sql,sqlParams)
  }

  def insert(areaTop3ProductArray:Array[AreaTop3Product]) ={

    val sql = "insert into area_top3_product values(?,?,?,?,?)"
    val sqlParmamsArray = new Array[Array[Any]](areaTop3ProductArray.length)

    for(i <- areaTop3ProductArray.indices){
      val areaTop3Product: AreaTop3Product = areaTop3ProductArray(i)
      sqlParmamsArray(i) = Array[Any](
        areaTop3Product.task_id,
        areaTop3Product.province,
        areaTop3Product.product_id,
        areaTop3Product.cart_count,
        areaTop3Product.city_infos
      )
    }

    JDBCHelper.executeBatch(sql,sqlParmamsArray)

  }


}
