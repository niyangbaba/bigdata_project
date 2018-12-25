package com.dxh.test.Test

/**
  * Created by Administrator on 2018/12/21.
  */
object IPToLong {

  def main(args: Array[String]): Unit = {
    val ip = "192.168.233.201"
    val ips = ip.split("[.]")
    var numIP = 0L
    for (i <- 0 until(ips.length)){
     numIP =  (numIP<<8) | ips(i).toLong
    }
    println(numIP)

   val a = ips(0).toLong*256*256*256+ips(1).toLong*256*256+ips(2).toLong*256+ips(3).toLong
    println(a)
  }
}
