package com.dataengine.spark.yy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NullIf
import org.apache.spark.sql.types.StringType

/**
  * create by roy 20210407
  *
  * 统计yarn queue配置信息并关联出业务组信息
  */
object ReadYarnQueueInfo {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
      .getOrCreate()
    val queueInfoDF = spark.read.format("csv").option("sep", ",").option("inferSchema", "true")
      .option("header", "true")
      .load("E:/doc/yy_dev/scheduler_config.csv")
    import spark.implicits._
    queueInfoDF.show(false)
    //过滤数据
    val name2DF = queueInfoDF.filter(line => {
      val names = line.getAs[String]("name").split("\\.")
      val k = names(1)
      names.size == 2 && (k.equals("capacity") || k.equals("maximum-capacity") || k.equals("priority"))
    }).map(row => {
      //转换为列的对象
      val names = row.getAs[String]("name").split("\\.")
      QueueInfo(names(0), names(1), row.getAs[String]("value"))
    }).toDF()
    //列转行
    name2DF.sort("queueName").show(false)
    import org.apache.spark.sql.functions._
    val pivotDF = name2DF.groupBy("queueName")
      .pivot("queyeKey")
      .agg(sum("value"))
    pivotDF.sort(desc("priority")).show(20, false)
    println(pivotDF.count())
    pivotDF.createOrReplaceTempView("queue_table")
    //联接出相关业务组
    val queueBusNameDF = spark.read.format("json").load("E:/doc/yy_dev/queue_bus_src.json")
    var queueBusNameDF2 = queueBusNameDF.select(explode(col("busqueues")).alias("cname")).select("cname.*")
    //去掉二级的后辍,取巧,如果不是这个归则 是无法找到相关业务的
    val queuenameUdf = udf((queuenameP: String) => {
      val queuename = queuenameP match {
        case null => ""
        case _ => queuenameP.toString
      }
      //不是这个规则的需要转换一下
      var reutrnV = queuename match {
        case "yule_vip" | "yule_normal" => "yy"
        case "rs_ec_vip" | "rs_ec_normal" => "liverecommand"
        case "zhgame_vip" | "zhgame_normal" => "yygame"
        case "hiido_vip" | "hiido_normal" => "hiido_inner"
        case "kaixindou_manual" | "hiido_normal" => "kaixindou"
        case "noizz_normal" | "noizz_vip" | "rs_ec_vip" | "rs_ec_normal" => "liverecommand"
        case "hiido" | "hiidoagent" | "hiidoagentlimit" | "hiidolimit" | "hiidoml" | "hiidovip" | "kaixindouvip" | "push" | "yule" | "yulevip" | "zhgame" => "overdue"
        case "push_normal" | "push_vip" => "push_inner"
        case _ => queuename
      }
      reutrnV.replace("_vip", "").replace("_normal", "").replace("_manual", "")
    })
    queueBusNameDF2 = queueBusNameDF2.filter(x => {
      val bname = x.getAs[String]("busname")
      !bname.equals("NULL")
      //if($"columnA"=="") lit(null) else $"columnA"
    }).withColumn("firstGradeQueueName", queuenameUdf(if ($"queuename" == null) lit("") else $"queuename".cast(StringType)))
    queueBusNameDF2.show(false)
    queueBusNameDF2.createOrReplaceTempView("bus_table")
    val resultDF = spark.sql(
      """
        |select t1.queueName,t2.busname busName,
        |t1.capacity,
        |t1.`maximum-capacity` maximumCapacity,
        |t1.priority
        |from queue_table t1
        |left join  bus_table t2
        |on t1.queueName=t2.firstGradeQueueName
        |""".stripMargin)
    resultDF.show(100, false)
    println(resultDF.count())
    //分组,把所属业务合一行
    resultDF.createOrReplaceTempView("result_tab")
    val resultDF2 = spark.sql(
      """
        |select queueName  as `queueName(队列名)`,concat_ws('|', collect_set(busName)) as `busNames(业务组)`,
        |max(capacity) `capacity(默认容量%)`,max(maximumCapacity) `maximumCapacity(最大容量%)`,
        |max(priority) `priority(优先级)`
        |from result_tab group by queueName
        |""".stripMargin)
    println("resultDF2 show=======")
    resultDF2.show(20, false)
    println(resultDF2.count())
    //输出统计好的文件
    resultDF2.sort(desc("`priority(优先级)`")).repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true").mode("overwrite").option("encoding", "gb2312")
      .save("yarn_queue_info_csv")
    spark.stop()
  }

  case class QueueInfo(queueName: String, queyeKey: String, value: String)

  case class QueueInfoToColumn(queueName: String, capacity: String, maximumCapacity: String, priority: String)

}
