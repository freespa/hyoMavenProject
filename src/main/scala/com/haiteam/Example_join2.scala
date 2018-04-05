package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_join2 {
  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").
    getOrCreate()

  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var selloutDb2 = "kopo_region_mst"

  val selloutDataFromorcl= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

  selloutDataFromorcl.createOrReplaceTempView("selloutTable")

  val selloutDataFromorcl2= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

  selloutDataFromorcl2.createOrReplaceTempView("selloutTable2")

  spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from selloutTable a " +
    "left join selloutTable2 b " +
    "on a.regionid = b.regionid")

}
