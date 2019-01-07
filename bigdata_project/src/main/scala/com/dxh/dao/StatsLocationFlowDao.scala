package com.dxh.dao

import com.dxh.bean.StatsLocationFlow
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2019/1/7.
  */
object StatsLocationFlowDao {

  /**
    * 实时更新流量统计结果
    *
    * @param statsLocationFlowArray
    */
  def update(statsLocationFlowArray: Array[StatsLocationFlow]) = {

    val sql =
      """
        |insert into stats_location_flow(date_dimension_id,location_dimension_id,nu,uv,pv,sn,`on`,created)values(?,?,?,?,?,?,?,?)
        |on duplicate key update nu=?,uv=?,pv=?,sn=?,`on`=?
      """.stripMargin

    val sqlParamsArray = new Array[Array[Any]](statsLocationFlowArray.length)

    for (i <- sqlParamsArray.indices) {

      val statsLocationFlow: StatsLocationFlow = statsLocationFlowArray(i)

      sqlParamsArray(i) = Array[Any](
        statsLocationFlow.date_dimension_id,
        statsLocationFlow.location_dimension_id,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on,
        statsLocationFlow.created,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on)
    }

    JDBCHelper.executeBatch(sql,sqlParamsArray)

  }

}
