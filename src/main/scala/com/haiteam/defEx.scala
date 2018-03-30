package com.haiteam

import org.apache.spark.sql.SparkSession

object defEx {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  def discountedPrice(price:Double, rate:Double) :Double = {
    var discount = price * rate
    var returnValue = price - discount
    returnValue

  }
  var orgrRate = 0.2
  var orgPrice = 2000
  var newPrice = discountedPrice(orgPrice, orgrRate)


  var a = 15.125222
  var b = 15.147218
  var c = 69.72756

  var ra = Math.round(a*100)/100.0
  var rb = Math.round(b*100)/100.0
  var rc = Math.round(c*100)/100.0

def hoho(ho1:Double) :Double = {
  var hoho3 = Math.round(a*100)/100.0
  hoho3
}
  var a1= 15.125222
 hoho(a1)

}
