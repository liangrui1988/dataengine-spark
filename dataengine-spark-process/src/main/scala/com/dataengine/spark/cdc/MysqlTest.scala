package com.dataengine.spark.cdc

import com.dataengine.spark.core.utils.ResourcesUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object MysqlTest {


  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {

    println("args==" + args)
    var Array(binlogIndex, binlogFileOffset, tableNamePattern) = (args ++ Array(null, null, null)).slice(0, 3)
    print(binlogIndex, binlogFileOffset, tableNamePattern)

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      //.master("local[*]")
      .getOrCreate()

    val (user, passwd, url, host, prot) = ResourcesUtils.getloopsActivityPropValues()
    println("Test read")
    println(user, passwd, url, host, prot)
    val db = "new_loops_activity"
    val table = "dlta_tab"
    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      //      option("binlog.field.decode.first_name", "UTF-8").
      option("host", host).
      option("port", prot).
      option("userName", user).
      option("password", passwd).
      option("bingLogNamePrefix", "mysql-bin-changelog").
      option("binlogIndex", binlogIndex).
      option("binlogFileOffset", binlogFileOffset).
      option("databaseNamePattern", "new_loops_activity").
      option("tableNamePattern", tableNamePattern).
      load()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .option("mode", "Append")
      //      .format("console")
      .format("csv")
      .option("path", "/tmp/msyqlbinlog/sink_file1")
      //    format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      //    option("__path__","/tmp/datahouse/{db}/{table}").
      //      option("path","{db}/{table}").
      //      option("mode","Append").
      //      option("idCols","id").
      //      option("duration","3").
      //      option("syncType","binlog").
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .option("checkpointLocation", "/tmp/binlogConsoleTestd")
      .start()
    query.awaitTermination()
    spark.stop()
  }

}
