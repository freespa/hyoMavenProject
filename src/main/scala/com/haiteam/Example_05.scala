package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_05 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


  var a = "jdbc:postgresql://192.168.110.111:5432/kopo";
      var b = "kopo";
  var c = "kopo";
  var d = "kopo_batch_season_mpara"

    val e = spark.read.format("jdbc").options(Map("url" -> a,"dbtable" ->d,"user" ->b, "password"-> c)).load

      println("load success")
    e.createOrReplaceTempView("table1")
  //////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.110.11:1522/XE"

    var staticUser = "HJ"
    var staticPw = "HJ"
    println("load success")
    var prop = new java.util.Properties
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    prop.setProperty("user",staticUser)
    prop.setProperty("password",staticPw)
    var table = "hyohyohyo"
   e.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("success")















  }
}