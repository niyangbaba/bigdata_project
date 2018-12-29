package com.dxh.dao

import com.dxh.bean.Top5Category
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2018/12/28.
  */
object Top5CategoryDao {

  /**
    * 删除taskID对应的任务
    * @param taskId
    */
  def deleteByTaskId(taskId :Int) = {

    val sql = "delete from top5_category where task_id=?"
    val sqlParams = Array[Any](taskId)
    JDBCHelper.executeUpdate(sql,sqlParams)

  }


  def insert(top5CategoryArray: Array[Top5Category])= {

    val sql = "insert into top5_category values(?,?,?,?,?,?)"

    val sqlParamsArray = new Array[Array[Any]](top5CategoryArray.length)

    for (i <- sqlParamsArray.indices){

      val top5Category = top5CategoryArray(i)
      sqlParamsArray(i) = Array[Any](
        top5Category.taskId,
        top5Category.category_id,
        top5Category.click_count,
        top5Category.cart_count,
        top5Category.order_count,
        top5Category.pay_count
      )

    }

    JDBCHelper.executeBatch(sql,sqlParamsArray)

  }


}
