package com.study.conf

import org.apache.hadoop.conf.Configuration

/**
  * Created by Administrator on 2018/12/29.
  */
object ConfigurManager {

  private val configuration = new Configuration()
  configuration.addResource("core-site.xml")
  configuration.addResource("hbase-site.xml")
  configuration.addResource("mysql-site.xml")
  configuration.addResource("project-site.xml")

  def getValue(name : String) = {
    configuration.get(name)
  }

}


