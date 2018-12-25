package com.dxh.test.Test

/**
  * Created by Administrator on 2018/12/21.
  */
object halfSearch {
  def main(args: Array[String]): Unit = {


    val ips = Array(
      (1,10,"上海"),
      (11,20,"北京"),
      (21,30,"河南"),
      (31,40,"武汉"),
      (41,50,"河北"),
      (51,60,"重庆"),
      (61,70,"广州")
    )

    val num = 1

    val index = search(ips,num)

    println(ips(index)._3)


  }

  def search(arr:Array[(Int,Int,String)],ip:Int) : Int= {

    var startIndex = 0
    var endIndex = arr.length -1

    while(startIndex <= endIndex){
      val midIndex = (startIndex + endIndex) / 2
      if (ip>=arr(midIndex)._1 && ip<=arr(midIndex)._2)
       return  midIndex
      else if (ip<arr(midIndex)._1)
        endIndex = midIndex -1
      else if (ip >= arr(midIndex)._2)
        startIndex = midIndex +1
    }
    return -1
  }

}
