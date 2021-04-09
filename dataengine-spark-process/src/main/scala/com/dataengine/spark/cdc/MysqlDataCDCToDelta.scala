package com.dataengine.spark.cdc

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * create by juzhu on 2020/08/24
 * 会员信息导出功能
 *
 */
object MysqlDataCDCToDelta {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    println("args==" + args)
    var Array(dt, filename_dt) = (args ++ Array(null, null)).slice(0, 2)
    println("args==", dt, filename_dt)

    val cal = Calendar.getInstance
    val cdt = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    if (StringUtils.isBlank(dt)) {
      dt = cdt
    }

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
     // .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.stop()
  }


}
