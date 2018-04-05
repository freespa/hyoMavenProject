package com.haiteam


import org.apache.spark.sql.SparkSession

object functionex1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var dataPath = "./data/"
    var selloutFile = "kopo_channel_seasonality_ex.csv"

    // 상대경로 입력
    var selloutData = spark.read.format("csv").option("header", "true").load(dataPath + selloutFile)




      }
}
