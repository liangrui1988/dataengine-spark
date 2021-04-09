package com.dataengine.spark.core.constant

/**
  * create roy by 2019/10/16
  * 数据库相关常量
  */

object DataDBConstant {

  /**
    * 老乡鸡db名称
    */
  val ODS_MONGODB_LXJ_DATABASE = "ods_m6_db"
  //  val ODS_MONGODB_LXJ_DATABASE = "ods_m6_db_test"


  /**
    * 中台日志，最新的转换接口后的库
    */
  val ods_middle_latest = "ods_middle_db"

  /**
    * 扩展列名 json string
    */
  val cd_ext_col = "cd_maps"

  /**
    * 扩展列名 map
    */
  val cd_ext_map_col = "cd_maps2"

  /**
    * 按小时分区列名,2019101812
    */
  val date_hour_partition = "phour"


  /**
    * 条数 2千万*10
    */
  val sample_size = "144535546"//200000000
}
