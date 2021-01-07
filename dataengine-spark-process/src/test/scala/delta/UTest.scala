package delta

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test


class UTest {
  Logger.getRootLogger.setLevel(Level.WARN)

  val spark = SparkSession.builder()
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  @Test
  def test1() {
    print("Test1")
    val data = spark.range(0, 5)
    data.write.format("delta").mode(SaveMode.Overwrite).save("/tmp/delta-table")
    spark.stop()
  }

  @Test
  def test2() {
    print("Test1")
    val df = spark.read.format("delta").load("/tmp/delta-table")
    df.show()
    spark.stop()
  }

}
