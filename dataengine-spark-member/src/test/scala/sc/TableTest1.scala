package sc

import com.yx.sy.core.utils.ResourcesUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

object TableTest1 {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ("local".equals(ResourcesUtils.getEnv("env"))) {
      sparkBuilder.master("local[*]").config("hive.metastore.uris", "thrift://10.200.102.187:9083")
    }

    val spark = sparkBuilder.appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()
    val tDF = spark.sql("select * from dw_middle_db.edwd_dday_mid_order_addorder_discounts")
    tDF.show()
    tDF.printSchema()


  }

}
