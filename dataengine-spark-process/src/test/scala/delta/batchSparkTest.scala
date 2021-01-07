package delta

import com.dataengine.spark.core.utils.ResourcesUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.junit.Test


class batchSparkTest {
  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()



  @Test
  def initDelta() {
    val tab="tab_val"
    val mysqlConf = Map(
      "url" -> "jdbc:mysql://127.0.0.1:3306?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "",
      "dbtable" -> s"(SELECT * FROM test.`${tab}` limit 100) as dlta_tab_tmp"
    )
    import org.apache.spark.sql.functions.col
    var df = spark.read.format("jdbc").options(mysqlConf).load()
    df = df.repartitionByRange(2, col("id"))
    df.show()
    df.write
      .format("delta").
      mode("overwrite").
      save(s"/tmp/datahouse/test/${tab}")
    spark.close()
  }

  @Test
  def testShowData2() {
    print("testShowData===")
    val df = spark.read.format("delta").load(s"/tmp/datahouse/test/tab_val")
    df.show()
    spark.close()

  }

  @Test
  def getData() {
    val (user, passwd, url, host, prot) = ResourcesUtils.getloopsActivityPropValues()
    print(user,url)
    val mysqlConf = Map(
      "url" -> url,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> user,
      "password" -> passwd,
      "dbtable" -> "(SELECT * FROM `dlta_tab` limit 10) as dlta_tab_tmp"
    )
    import org.apache.spark.sql.functions.col
    var df = spark.read.format("jdbc").options(mysqlConf).load()
    df = df.repartitionByRange(2, col("id"))
    df.show()
    df.write
      .format("delta").
      mode("overwrite").

      save("/tmp/datahouse/delta/batch_dlta_tab")
    spark.close()
  }

  @Test
  def testShowData() {
    print("testShowData===")
    val df = spark.read.format("delta").load("/tmp/datahouse/delta/batch_dlta_tab")
    df.show()
    spark.close()

  }


}
