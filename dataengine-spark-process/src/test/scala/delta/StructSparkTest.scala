package delta

import com.dataengine.spark.core.utils.ResourcesUtils
import com.dataengine.spark.core.utils.ResourcesUtils.getPropValues
import org.apache.log4j.{Level, Logger}
import org.apache.maven.wagon.PathUtils.user
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.junit.Test


class StructSparkTest {

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()


  @Test
  def writerDeltaLake() {
    val (user, passwd, url, host, prot) = ResourcesUtils.getloopsActivityPropValues()
    println("Test read")
    println(user, passwd, url, host, prot)

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("binlog.field.decode.first_name", "UTF-8").
      option("host", "127.0.0.1").
      option("port", "3306").
      option("userName", "root").
      option("password", "").
      option("useSSL", "false").
      option("bingLogNamePrefix", "log_bin").
      option("binlogIndex", "2").
      option("binlogFileOffset", "5542").
      option("databaseNamePattern", "test").
      option("tableNamePattern", "delta_test|tab_val").
      option("serverId", "1").
      load()


    val query = df.writeStream.
      format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
      option("__path__","/tmp/datahouse/{db}/{table}").
      option("path","{db}/{table}").
      option("mode","Append").
      option("idCols","id").
      option("duration","3").
      option("syncType","binlog").
      option("checkpointLocation", "/tmp/cpl-binlog8").
      outputMode("append")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    query.awaitTermination()
    spark.stop()

  }


  @Test
  def readLocal() {
    println("readLocal")

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
      option("binlog.field.decode.first_name", "UTF-8").
      option("host", "127.0.0.1").
      option("port", "3306").
      option("userName", "root").
      option("password", "").
      option("useSSL", "false").
      option("bingLogNamePrefix", "log_bin").
      option("binlogIndex", "2").
      option("binlogFileOffset", "1510").
      option("databaseNamePattern", "test").
      option("tableNamePattern", "delta_test|tab_val").
      option("serverId", "1").
      load()

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .option("mode", "Append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "100000")

    //.format("csv")
     // .option("path", "/tmp/msyqlbinlog/sink_file1")
      .option("checkpointLocation", "/tmp/binlogConsoleTestmysql2")
      .start()
    query.awaitTermination()
    spark.stop()

  }


}
