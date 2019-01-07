package com.study.task

import com.dxh.caseclass.IPRule
import com.dxh.constants.LogConstants
import com.study.conf.ConfigurManager
import com.study.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

/**
  *
  * AnalysisLog 分析日志
  *
  */
object AnalysisLog extends BaseTask {

  private var inputDate: String = _
  private var inputPath: String = _

  private def checkArgs(args: Array[String]) = {

    //判断长度
    if (args.length == 0) {
      println(
        """
          |USER:com.study.task.AnalysisLog
          |INFO:Saprk task requires at least one parameter!
        """.stripMargin)
    }
    System.exit(0)

    //判断参数的类型 是否是 yyyy-MM-dd
    if (Utils.isDate(args(0))) {
      println(
        """
          |USER:com.study.task.AnalysisLog
          |INFO:The input date format should be yyyy - MM - dd
        """.stripMargin)
      System.exit(0)
    }

    inputDate = args(0)

  }

  private def checkHdfsPath(args: Array[String]) = {

    //对日期进行转换 2018-10-18 -> /logs/2018/10/18
    inputPath = "/logs/" + Utils.formatDate(Utils.parseDate(inputDate))

    var fileSystem: FileSystem = null

    try {
      fileSystem = FileSystem.newInstance(conf)

      if (!fileSystem.exists(new Path(inputPath))) {
        println(
          """
            |USER:com.study.task.AnalysisLog
            |INFO:HDFS does not exist the path
          """.stripMargin)
        System.exit(0)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (fileSystem != null) {
        fileSystem.close()
      }
    }
  }

  private def loadIpRulers() = {
    sc.textFile(ConfigurManager.getValue("project_resource"), 2).map(line => {
      //line==>1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val splited = line.split("[|]")
      IPRule(splited(2).toLong, splited(3).toLong, splited(5), splited(6), splited(7))
    }).collect()
    //collect() 将结果拉回driver端
  }

  private def loadIpToMap(str: String, logMap: mutable.Map[String, String]) = {

  }

  /**
    *
    * @param logText
    *124.207.252.34|1539825800.145|119.23.63.188:83|/log.gif?en=e_pv&ver=1&pl=pc&os_n=win10&b_n=chrome&b_v=55.0.2883.87&
    * @param value
    *1539825800.145==>1539825800145
    */
  private def analysisLogInMethod(logText: String, value: Array[IPRule]) = {

    var logMap: mutable.Map[String, String] = null

    if (StringUtils.isNotBlank(logText)) {

      val params = logText.split("[|]")

      if (params.length == 4) {

        logMap =mutable.Map[String,String]()

        loadIpToMap(params(0),logMap)
        logMap.put(LogConstants.lOG_COLUMNS_NAME_ACCESS_TIME,(params(1).toDouble * 1000).toLong.toString)


      }

    }


  }

  private def analysisLogToMap(ipRulesBroadcast: Broadcast[Array[IPRule]]) = {
    sc.textFile(inputPath).map(logText => {
      analysisLogInMethod(logText, ipRulesBroadcast.value)
    })
  }

  def main(args: Array[String]): Unit = {

    //1.对参数进行判定
    checkArgs(args)
    //2.验证当前日期 HDFS是否存在日志
    checkHdfsPath(args)
    //3.使用spark加载ip规则库
    val ipRules: Array[IPRule] = loadIpRulers()
    val ipRulesBroadcast = sc.broadcast(ipRules)
    //4.日志分析 存入到Map中去
    analysisLogToMap(ipRulesBroadcast)


  }

}
