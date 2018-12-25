package com.dxh.bean

/**
  * Created by Administrator on 2018/12/25.
  */
class SparkTask(var task_id: Int, var task_param: String) {
  override def toString: String = {
    s"SparkTask($task_id, $task_param)"
  }
}
