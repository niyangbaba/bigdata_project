package com.dxh.task.session

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * UDF : 即用户自定义函数，对单行进行处理
  * UDAF : 即用户自定义聚合函数，对多行记录进行处理，聚合成一行
  */
class CityConcatUDAF extends UserDefinedAggregateFunction {


  //输入值的数据类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("city", StringType, true)))
  }

  //每一次聚合后结果的数据类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("cities", StringType, true)))
  }

  //聚合完后最终结果的数据类型
  override def dataType: DataType = {
    StringType
  }

  //每次聚合后的结果类型与最终结果的数据类型是否一致
  override def deterministic: Boolean = {
    true
  }

  //初始化值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  //单节点上 数据聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //取出原来的结果
    val oldValue = buffer(0)
    var newValue = oldValue + "," + input(0)

    if (newValue.startsWith(",")) {
      newValue = newValue.substring(1)
    }

    buffer.update(0, newValue)

  }

  //多节点上数据聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val oldValue = buffer1(0)
    var newValue = oldValue + "," + buffer2(0)

    if (newValue.startsWith(",")){
      newValue = newValue.substring(1)
    }

    buffer1.update(0,newValue)
  }

  //最终的数据输出
  override def evaluate(buffer: Row): Any = {
    buffer(0)

  }
}
