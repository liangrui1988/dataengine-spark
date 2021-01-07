package sc

import com.yx.sy.core.utils.ResourcesUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object PartDropTest {

  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ("local".equals(ResourcesUtils.getEnv("env"))) {
      System.setProperty("HADOOP_USER_NAME", "root")
      sparkBuilder.master("local[*]").config("hive.metastore.uris", "thrift://10.200.102.187:9083")
    }

    val spark = sparkBuilder.appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()
    val queryDF=spark.sql("select * from test.part_drop_tab where barnd='barnd3'")
//    queryDF.cache()
    queryDF.show()
    spark.sql(s"ALTER TABLE test.part_drop_tab DROP IF EXISTS PARTITION(barnd='barnd3')")
    queryDF.show()

    queryDF.write.mode("append").partitionBy("barnd").saveAsTable("test.part_drop_tab")
  }
}
