package com.dataengine.spark.yy

import com.dataengine.spark.core.utils.ResourcesUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object StatBusQueue {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
      .getOrCreate()
    import org.apache.spark.sql.functions._

    val queueBusNameDF = spark.read.format("json").load("E:/doc/yy_dev/queue_bus_src.json")
    val rowDF = queueBusNameDF.select(explode(col("busqueues")).alias("cname")).select("cname.*")
    import spark.implicits._
    val rdd = rowDF
    rdd.show(false)





    spark.stop()

  }

}
