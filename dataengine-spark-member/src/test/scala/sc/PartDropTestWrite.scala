package sc

import com.yx.sy.core.utils.ResourcesUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object PartDropTestWrite {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder().master("local[*]")
//    if ("local".equals(ResourcesUtils.getEnv("env"))) {
//      System.setProperty("HADOOP_USER_NAME", "root")
//      sparkBuilder.master("local[*]").config("hive.metastore.uris", "thrift://10.200.102.187:9083")
//    }

    val spark = sparkBuilder.appName(this.getClass.getName)
//      .enableHiveSupport()
      .getOrCreate()
    val data = Seq(
      Row("barnd1", "1", "a", 300.0),
      Row("barnd1", "1", "a", 300.0),
      Row("barnd1", "1", "b", 200.0),
      Row("barnd2", "2", "c", 200.0),
      Row("barnd2", "2", "c", 200.0),
      Row("barnd2", "1", "c", 200.0),
      Row("barnd2", "2", "a", 200.0),
      Row("barnd3", "1", "c", 200.0),
      Row("barnd3", "1", "c", 200.0)
    )
    val schme = new StructType()
      .add("barnd", StringType)
      .add("type", StringType)
      .add("orderid", StringType)
      .add("price", DoubleType)
    schme.printTreeString()
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schme)
//     df.write.mode("overwrite").partitionBy("barnd").saveAsTable("test.part_drop_tab")
    df.show()
    val titleCol="""barnd ,type""""
    val colsSeq=titleCol.split(",")
    import org.apache.spark.sql.functions.col

    df.select(colsSeq.map(col(_)):_*).show()

//    spark.sql("select * from test.part_drop_tab").show()
  }

}
