package com.dxh.conf

import org.apache.hadoop.conf.Configuration

/**
  * Created by Administrator on 2018/12/24.
  */
object ConfigurationManage {

  private val configuration = new Configuration()
  configuration.addResource("mysql-site.xml")
  configuration.addResource("project-site.xml")


  def getProperty(name :String) ={

    configuration.get(name)

  }


}
