package com.yx.sy.mb

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dataengine.spark.core.utils.ResourcesUtils
import com.dataengine.spark.fun.DictUdf
import com.dataengine.spark.utils.SyFileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * create by juzhu on 2020/08/24
 * 会员信息导出功能
 *
 */
object ExportMemberInfo {
  def main(args: Array[String]): Unit = {

//    println("args==" + args)
//    var Array(dt, filename_dt) = (args ++ Array(null, null)).slice(0, 2)
//    println("args==", dt, filename_dt)
//
//    val cal = Calendar.getInstance
//    val cdt = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
//    if (StringUtils.isBlank(dt)) {
//      dt = cdt
//    }
//    if (StringUtils.isBlank(filename_dt)) {
//      filename_dt = s"member_data_${dt}"
//    }
//    //    val filename_dt = s"member_data_${cdt}_${spark.sparkContext.applicationId}"
//    print("ExportMemberInfo start run==============env", ResourcesUtils.getEnv("env"))
//    val sparkBuilder = SparkSession.builder()
//    if ("local".equals(ResourcesUtils.getEnv("env"))) {
//      sparkBuilder.master("local[*]")
//      //.config("hive.metastore.uris", "thrift://10.200.102.187:9083")
//    }
//    val spark = sparkBuilder
//      .appName(this.getClass.getName)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    //累计储值金额-账户余额=累计使用储值金额
//
//    def catalog =
//      s"""{
//         |"table":{"namespace":"SAAS", "name":"DWS_IM_USER_WIDE_TBL"},
//         |"rowkey":"key",
//         |"columns":{
//         |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
//         |"会员ID":{"cf":"ST", "col":"MEMBER_ID", "type":"string"},
//         |"卡号":{"cf":"ST", "col":"MEMBER_CARD_NO", "type":"string"},
//         |"姓名":{"cf":"ST", "col":"REAL_NAME", "type":"string"},
//         |"性别":{"cf":"ST", "col":"GENDER", "type":"tinyint"},
//         |"生日":{"cf":"ST", "col":"BIRTHDAY", "type":"string"},
//         |"手机号":{"cf":"ST", "col":"PHONE_NUMBER", "type":"string"},
//         |"会员状态":{"cf":"ST", "col":"MEMBER_STATUS", "type":"string"},
//         |"注册渠道编码":{"cf":"ST", "col":"CHANNEL_CODE", "type":"string"},
//         |"注册门店编码":{"cf":"ST", "col":"STORE_CODE", "type":"string"},
//         |"注册门店":{"cf":"ST", "col":"STORE_NAME", "type":"string"},
//         |"首次消费门店编码":{"cf":"ST", "col":"FIRST_ORDER_SHOPID", "type":"string"},
//         |"首次消费门店":{"cf":"ST", "col":"FIRST_ORDER_SHOPNAME", "type":"string"},
//         |"入会天数":{"cf":"ST", "col":"REGISTER_DAYS", "type":"int"},
//         |"order_amount":{"cf":"EXT", "col":"ORDER_AMOUNT", "type":"double"},
//         |"order_discount":{"cf":"EXT", "col":"ORDER_DISCOUNT", "type":"double"},
//         |"累计消费次数":{"cf":"EXT", "col":"ORDER_NUM", "type":"int"},
//         |"最近120天平均客单价":{"cf":"EXT", "col":"FULL_AVERAGE_PRICE_120D", "type":"double"},
//         |"当前成长值":{"cf":"EXT", "col":"GROWTH_VALUE", "type":"int"},
//         |"等级":{"cf":"ST", "col":"LEVEL_NAME", "type":"string"},
//         |"已使用积分":{"cf":"EXT", "col":"EXCHANGE_POINTS", "type":"int"},
//         |"积分兑换次数":{"cf":"EXT", "col":"YH_POINTS_COUNT", "type":"string"},
//         |"积分参与活动次数":{"cf":"EXT", "col":"ACTIVITIES_POINT_COUNT", "type":"int"},
//         |"剩余积分":{"cf":"EXT", "col":"AVAILABLE_POINTS", "type":"int"},
//         |"已过期积分":{"cf":"EXT", "col":"EXPIRED_POINTS", "type":"int"},
//         |"累计充值金额":{"cf":"EXT", "col":"TOTAL_CHARGE_AMOUNT", "type":"double"},
//         |"账户余额":{"cf":"EXT", "col":"BALANCE", "type":"double"},
//         |"注册时间":{"cf":"ST", "col":"REGISTER_TIME", "type":"string"},
//         |"最近消费时间":{"cf":"EXT", "col":"LST_ORDER_TIME", "type":"string"}
//         |}
//         |}""".stripMargin
//    def withCatalog(cat: String): DataFrame = {
//      spark
//        .read
//        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
//        .format("org.apache.spark.sql.execution.datasources.hbase")
//        .load()
//    }
//
//    val df = withCatalog(catalog)
//    print("data show start")
//    df.show(200, false)
//    //数据处理
//    //累计消费金额 ORDER_AMOUNT-ORDER_DISCOUNT
//    import org.apache.spark.sql.functions.udf
//    import spark.implicits._
//    val consume_money_udf = udf { (order_amount: Double, order_discount: Double) => {
//      (BigDecimal(order_amount) - BigDecimal(order_discount)).toDouble
//    }
//    }
//    val edtDF = df.withColumn("consume_money", consume_money_udf($"order_amount", $"order_discount"))
//      //累计使用储值金额= 累计充值金额-账户余额
//      .withColumn("lj_use_save_money", consume_money_udf($"累计充值金额", $"账户余额"))
//      .withColumn("性别", DictUdf.sexUdf($"性别"))
//      .withColumn("会员状态", DictUdf.memberStatusUdf($"会员状态")).na.fill(0, Seq("积分参与活动次数"))
//    //关联门店 dim_shp_sc_shop
//    spark.sql(
//      """
//        |select code,name from dtsaas.dim_shp_sc_shop
//        |""".stripMargin).createOrReplaceTempView("dim_shp_sc_shop_tmp")
//    //注册渠道编码 关联渠道名称
//    spark.sql(
//      """
//        |select code,name from dtsaas.dwb_dm_bd_code where group_id='member_register_channel'
//        |""".stripMargin).createOrReplaceTempView("member_register_channel_tmp")
//    spark.sql("select * from member_register_channel_tmp").show()
//
//    edtDF.createOrReplaceTempView("member_info_tmp_tab")
//    val sqlx =
//      """
//        |select
//        |concat('="',t1.`会员ID`,'"') `会员ID`,
//        |concat('="',t1.`卡号`,'"') `卡号`,
//        |t1.`姓名`,t1.`性别`,t1.`生日`,t1.`手机号`,t1.`会员状态`,
//        |t1.`注册渠道编码`,
//        |t3.name `注册渠道名称`,
//        |t1.`注册时间`,t1.`注册门店编码`,t1.`注册门店`,
//        |t1.`首次消费门店编码`,
//        |t2.name `首次消费门店`,
//        |t1.`入会天数`,
//        |t1.consume_money `累计消费金额` ,
//        |t1.`累计消费次数`,
//        |round(t1.`最近120天平均客单价`,2) as `最近120天平均客单价`,
//        |round((t1.`当前成长值`/100),0) `当前成长值`,
//        |t1.`等级`,
//        |t1.`已使用积分`,t1.`积分兑换次数`,t1.`积分参与活动次数`,t1.`剩余积分`,t1.`已过期积分`,t1.`累计充值金额`,
//        |t1.lj_use_save_money `累计使用储值金额`,t1.`最近消费时间`
//        | from  member_info_tmp_tab t1
//        | left join dim_shp_sc_shop_tmp t2 on t1.`首次消费门店编码`=t2.code
//        | left join member_register_channel_tmp t3 on t1.`注册渠道编码`=t3.code
//         """.stripMargin
//    println(sqlx)
//
//    //
//    val resultDF = spark.sql(sqlx)
//    //导出数据写文件
//    // to hdfs tmp file
//    val outputFileName = s"/user/spark/mermber_out_csvdir/tmp_${filename_dt}.csv"
//    //结果hdfs存放的路径
//    var statement_file_path = "/user/spark/mermber_out_csvdir_result/" + filename_dt + ".zip"
//    println("resultDF show ")
//    resultDF.show(false)
//    resultDF
//      .write
//      .format("csv").option("quote", "'")
//      .option("header", "false")
//      .mode("overwrite")
//      .save(outputFileName)
//    val headers = resultDF.schema.fieldNames.mkString(",")
//    println("headers==", headers)
//    SyFileUtils.merge(outputFileName, statement_file_path, filename_dt, headers)
//    spark.stop()
  }


}
