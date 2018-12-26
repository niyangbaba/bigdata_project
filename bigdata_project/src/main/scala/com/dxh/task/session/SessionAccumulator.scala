package com.dxh.task.session

import com.dxh.constants.GlobalConstants
import com.dxh.utils.Utils
import org.apache.spark.AccumulatorParam


/**
  * Created by Administrator on 2018/12/26.
  */
object SessionAccumulator extends AccumulatorParam[String] {


  /**
    * 初始化自定义累加器的值
    *
    * @param initialValue
    * session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0
    * @return
    */
  override def zero(initialValue: String): String = {
    GlobalConstants.SESSION_COUNT + "=" + 0 + "|" +
      GlobalConstants.TIME_1s_3s + "=" + 0 + "|" +
      GlobalConstants.TIME_4s_6s + "=" + 0 + "|" +
      GlobalConstants.TIME_7s_9s + "=" + 0 + "|" +
      GlobalConstants.TIME_10s_30s + "=" + 0 + "|" +
      GlobalConstants.TIME_30s_60s + "=" + 0 + "|" +
      GlobalConstants.TIME_1m_3m + "=" + 0 + "|" +
      GlobalConstants.TIME_3m_10m + "=" + 0 + "|" +
      GlobalConstants.TIME_10m_30m + "=" + 0 + "|" +
      GlobalConstants.TIME_30m + "=" + 0 + "|" +
      GlobalConstants.STEP_1_3 + "=" + 0 + "|" +
      GlobalConstants.STEP_4_6 + "=" + 0 + "|" +
      GlobalConstants.STEP_7_9 + "=" + 0 + "|" +
      GlobalConstants.STEP_10_30 + "=" + 0 + "|" +
      GlobalConstants.STEP_30_60 + "=" + 0 + "|" +
      GlobalConstants.STEP_60 + "=" + 0
  }

  /**
    * 定义每次如何进行累加
    *
    * @param v1
    * 1,自定义累加器的初始值或每次累加后的结果
    * session_count=4|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=1|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * session_count=5|1s_3s=1|4s_6s=1|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=1|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * 2,当v1代表的是累加器的初始值 ""
    * @param v2
    * 1，需要累加的字段  session_count
    * 2,v2代表的是最终结果v2
    * @return
    */
  override def addInPlace(v1: String, v2: String): String = {

    if (v1.equals("")) {
      v2
    } else {
      //获取字段原来的值
      val fieldValue = Utils.getFieldValue(v1, v2).toInt
      //对该字段的值进行累加+1
      val fieldNameNewValue = (fieldValue + 1).toString
      //累加完后重新赋值
      Utils.setFieldValue(v1, v2, fieldNameNewValue)
    }
  }

}
