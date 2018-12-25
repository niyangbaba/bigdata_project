package com.dxh.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.dxh.conf.ConfigurationManage
import com.dxh.constants.GlobalConstants

/**
  * Created by Administrator on 2018/12/24.
  */
object JDBCHelper {

  /**
    * 1，获取连接对象
    */
  def getConnection() = {

    var connection: Connection = null

    try {

      val driver = ConfigurationManage.getProperty(GlobalConstants.JDBC_DRIVER)
      val url = ConfigurationManage.getProperty(GlobalConstants.JDBC_URL)
      val user = ConfigurationManage.getProperty(GlobalConstants.JDBC_USER)
      val passWord = ConfigurationManage.getProperty(GlobalConstants.JDBC_PASSWORD)
      //注册JDBC驱动
      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, passWord)

    } catch {
      case e: Exception => e.printStackTrace()
    }

    connection
  }


  /**
    * 2，通用的增，删，该方法（单条记录）
    * insert into student(id,name,age)values(?,?,?)
    * Array(1,"xm",18)
    */
  def executeUpdate(sql: String, sqlParams: Array[Any]) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = getConnection()
      //获取执行sql的命令对象
      preparedStatement = connection.prepareStatement(sql)
      //对sql语句中的 ？进行赋值
      for (i <- 0 until (sqlParams.length)) {
        preparedStatement.setObject(i + 1, sqlParams(i))
      }
      //执行sql语句
      preparedStatement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }


  }

  /**
    * 3，通用的增，删，该方法（批处理）
    * insert into student(id,name,age)values(?,?,?)
    * Array( Array(1,"xm",18),Array(1,"xm",18),Array(1,"xm",18),Array(1,"xm",18),Array(1,"xm",18))
    */
  def executeBatch(sql: String, sqlParamsArray: Array[Array[Any]]) = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = getConnection()
      //设置手动提交事务
      connection.setAutoCommit(false)
      //获取执行sql的命令对象
      preparedStatement = connection.prepareStatement(sql)

      for (i <- 0 until (sqlParamsArray.length)) {
        val sqlParams = sqlParamsArray(i)
        for (j <- 0 until (sqlParams.length)) {
          //设置参数
          preparedStatement.setObject(j + 1, sqlParams(j))
        }
        //将执行的sql语句添加至batch内
        preparedStatement.addBatch()
      }
      //执行sql批处理
      preparedStatement.executeBatch()
      //提交事务
      connection.commit()
    } catch {
      case e: Exception => {
        connection.rollback()
        e.printStackTrace()
      }
    } finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }

  }

  /**
    * 4，通用的查询方法
    * 函数式编程
    * select id,name,age from student where id=?
    * Array(1)
    */
  def executeQuery(sql: String, sqlParams: Array[Any],func:ResultSet => Unit) = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null

    try {

      connection = getConnection()
      preparedStatement = connection.prepareStatement(sql)

      for(i <- 0 until(sqlParams.length)){
        preparedStatement.setObject(i+1,sqlParams(i))
      }

      resultSet = preparedStatement.executeQuery()
      func(resultSet)

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if (resultSet != null)
        resultSet.close()
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }


  }

}
