package com.dxh.dao

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import com.dxh.bean.{DateDimension, LocationDimension}
import com.dxh.jdbc.JDBCHelper

/**
  * Created by Administrator on 2019/1/4.
  */
object DimensionDao {

  /**
    * 日期信息 查询和插入 sql语句
    */
  private def buildDateSql() = {
    Array(
      "select id from dimension_date where `year`=? and season=? and `month`=? and `week`=? and `day`=? and calendar=? and type=?",
      "insert into dimension_date(`year`,season,`month`,`week`,`day`,calendar,type)values(?,?,?,?,?,?,?)"
    )
  }

  /**
    * 地域信息 查询和插入 sql语句
    */
  private def buildLocationSql() = {
    Array(
      "select id from dimension_location where country=? and province=? and city=?",
      "insert into dimension_location(country,province,city)values(?,?,?)"
    )
  }

  /**
    * 给sql赋值
    *
    * @param preparedStatement
    * @param dimension
    */
  private def setArgsForSql(preparedStatement: PreparedStatement, dimension: Any) = {

    if (dimension.isInstanceOf[DateDimension]) {
      val dateDimension = dimension.asInstanceOf[DateDimension]
      preparedStatement.setObject(1, dateDimension.year)
      preparedStatement.setObject(2, dateDimension.season)
      preparedStatement.setObject(3, dateDimension.month)
      preparedStatement.setObject(4, dateDimension.week)
      preparedStatement.setObject(5, dateDimension.day)
      preparedStatement.setObject(6, dateDimension.calendar)
      preparedStatement.setObject(7, dateDimension.dateType)
    } else if (dimension.isInstanceOf[LocationDimension]) {
      val locationDimension = dimension.asInstanceOf[LocationDimension]
      preparedStatement.setObject(1, locationDimension.country)
      preparedStatement.setObject(2, locationDimension.province)
      preparedStatement.setObject(3, locationDimension.city)
    }
  }

  /**
    * 执行sql语句
    *
    * @param sqlS
    * @param dimension
    * @param connection
    */
  private def executeSql(sqlS: Array[String], dimension: Any, connection: Connection) = {

    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val querySql = sqlS(0)
      preparedStatement = connection.prepareStatement(querySql)
      //给sql赋值
      setArgsForSql(preparedStatement, dimension)
      //执行查询sql
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        resultSet.getInt(1)
      } else {
        //查询不到，需要做插入操作
        val insertSql = sqlS(1)
        //获取 插入操作后返回的主键id Statement.RETURN_GENERATED_KEYS
        preparedStatement = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
        //给sql赋值
        setArgsForSql(preparedStatement, dimension)
        //执行插入操作sql  返回受影响的行数
        if (preparedStatement.executeUpdate() > 0) {
          resultSet = preparedStatement.getGeneratedKeys
          if (resultSet.next()) {
            resultSet.getInt(1)
          } else {
            throw new Exception("获取维度主键ID失败")
          }

        } else {
          throw new Exception("插入维度信息失败")
        }

      }

    } catch {
      case e: Exception => throw e
    } finally {
      if (resultSet != null)
        resultSet.close()
      if (preparedStatement != null)
        preparedStatement.close()
    }


  }

  /**
    * 获取时间或地域维度id
    *
    * @param dimension  时间或地域维度
    * @param connection 数据库连接对象
    */
  def getDimensionId(dimension: Any, connection: Connection) = {

    var sqlS: Array[String] = null

    if (dimension.isInstanceOf[DateDimension]) {
      sqlS = buildDateSql()
    } else if (dimension.isInstanceOf[LocationDimension]) {
      sqlS = buildLocationSql()
    }


    //在rdd中是并行执行的 并发
    synchronized({
      executeSql(sqlS, dimension, connection)
    })

  }

}
