package com.study.hbase

import org.apache.hadoop.conf.Configuration

/**
  * Created by Administrator on 2018/12/27.
  */
object getConfParams {

  def main(args: Array[String]): Unit = {
      val password = getParams("jdbc.password")
    println(password)
  }

  private val configuration = new Configuration()
  configuration.addResource("mysql-site.xml")
  configuration.addResource("hbase-site.xml")
  configuration.addResource("project-site.xml")


  def getParams(name: String) = {

    configuration.get(name)
  }

}
