package com.yx.sy.mb


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}



object DeltaTest1 {
//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
  //  System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      // .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
//   val data = spark.range(0, 5)
//    data.write.format("delta").save("/tmp/delta-table")
    val df = spark.read.format("delta").load("/tmp/delta-table")
    df.show()
    spark.stop()
  }


}
