package com.dataengine.spark.core.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileUtils {


  def readExecle(spark: SparkSession, path: String = "file:///E:/doc/mergerxls/order.xlsx", dataAddress: String): DataFrame = {
    val df = spark.read
      .format("com.crealytics.spark.excel")
      //      .option("sheetName", "Info")
      .option("useHeader", "true")
      .option("dataAddress", dataAddress) // Optional, default: "A1"
      //    .schema(peopleSchema)
      .load(path)
    df
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = readExecle(spark, "file:///E:/doc/jde/j吉野家jde分类汇总表.xlsx", "'分类信息'!A1")
    df.show(false)
    println(df.count())


  }

}
