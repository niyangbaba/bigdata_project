package com.dxh.dao

import com.dxh.bean.SessionAggrStat
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2018/12/28.
  */
object SessionAggrStatDao {

  /**
    * 删除 taskid对应的mysql上 的任务数据
    *
    * @param taskId
    */
  def deleteByTaskId(taskId: Int) = {
    val sql = "delete from session_aggr_stat where task_id=? "
    val sqlParams = Array[Any](taskId)
    JDBCHelper.executeUpdate(sql, sqlParams)
  }

  /**
    *
    * @param sessionAggrStat
    */
  def insert(sessionAggrStat: SessionAggrStat) = {
    val sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val sqlParams = Array[Any](
      sessionAggrStat.task_id,
      sessionAggrStat.session_count,
      sessionAggrStat.time_1s_3s,
      sessionAggrStat.time_4s_6s,
      sessionAggrStat.time_7s_9s,
      sessionAggrStat.time_10s_30s,
      sessionAggrStat.time_30s_60s,
      sessionAggrStat.time_1m_3m,
      sessionAggrStat.time_3m_10m,
      sessionAggrStat.time_10m_30m,
      sessionAggrStat.time_30m,
      sessionAggrStat.step_1_3,
      sessionAggrStat.step_4_6,
      sessionAggrStat.step_7_9,
      sessionAggrStat.step_10_30,
      sessionAggrStat.step_30_60,
      sessionAggrStat.step_60
    )
    JDBCHelper.executeUpdate(sql, sqlParams)
  }

}
