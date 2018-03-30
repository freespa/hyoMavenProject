package com.haiteam

import org.apache.spark.sql.SparkSession


object Forex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.8
    var priceDataSize = priceData.size
    var i = 0

    while(i < priceDataSize){
      var promotionEffect =Array(priceData(i) * promotionRate)
      i=i+1
    }



  for(i <-0 until priceDataSize){
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect

  }








  }

}


