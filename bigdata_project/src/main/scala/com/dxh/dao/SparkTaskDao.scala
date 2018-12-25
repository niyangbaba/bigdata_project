package com.dxh.dao

import java.sql.ResultSet

import com.dxh.bean.SparkTask
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2018/12/25.
  */
object SparkTaskDao {

  /**
    * select task_id,task_param from spark_task where task_id=?
    * 从MySQL中获取 task
    * @param taskId
    * @return
    */
  def getSparkTaskByTaskID(taskId: Int) = {
    var sparkTask: SparkTask = null
    val sql = "select task_id,task_param from spark_task where task_id = ?"
    val sqlParams = Array[Any](taskId)
    JDBCHelper.executeQuery(sql, sqlParams, (resultSet: ResultSet) => {
      if (resultSet.next()) {
        sparkTask = new SparkTask(resultSet.getInt("task_id"), resultSet.getString("task_param"))
      }
    })
    sparkTask
  }


}
