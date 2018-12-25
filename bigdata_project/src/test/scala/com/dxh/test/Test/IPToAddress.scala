package com.dxh.test.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/12/21.
  */
object IPToAddress {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new  SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ipRdd: RDD[String] = sc.textFile("F:\\数据\\数据四\\hadoop4\\day00\\sparkData\\ip.txt")

    val ipArr = ipRdd.map(line => {
      //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
      val splited = line.split("[|]")
      val startIp = splited(2).trim.toLong
      val endIp = splited(3).trim.toLong
      val country = splited(5)
      val province = splited(6)
      val city = splited(7)
      (startIp, endIp, country, province, city)
    }).collect()

    val ipBroadcast = sc.broadcast(ipArr)

    val logRdd = sc.textFile("F:\\实训二\\day04\\jd.log")

    logRdd.map(line => {
      val ip = line.split("[|]")(0)
      val longIp = ipToLong(ip)

      val ipArr = ipBroadcast.value

      val index = search(ipArr,longIp)

      (ip,ipArr(index))

    }).foreach(println(_))


    sc.stop()
  }


  def search(ipArr:Array[(Long,Long,String,String,String)],longIp:Long) :Int = {
    var startIndex = 0
    var endIndex = ipArr.length -1

    while(startIndex <= endIndex){
      val midIndex = (startIndex + endIndex) /2

      if (longIp >= ipArr(midIndex)._1 && longIp <= ipArr(midIndex)._2){
         return midIndex
      }else if (longIp < ipArr(midIndex)._1){
          endIndex = midIndex -1
      } else if(longIp > ipArr(midIndex)._2){
          startIndex = midIndex + 1
      }
    }
    return -1
  }

  def ipToLong(ip:String)= {
    val ips = ip.split("[.]")
    var numIp = 0L
    for (i <- 0 until(ips.length)){
      numIp = (numIp<<8) | ips(i).toLong
    }
    numIp
  }

}
