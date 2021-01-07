package com.dataengine.spark.fun

import org.apache.spark.sql.functions._

/**
 * create by juzhu on 2020/08/27
 * 通用字典udf
 */
object DictUdf {

  /**
   * 性别1女 2男
   */
  val sexUdf = udf { (sex: Int) => {
    sex match {
      case 1 => "女"
      case 2 => "男"
      case _ => "未知"
    }
  }
  }

  /**
   * 会员状态
   * '状态 0=未激活  1=激活 2=冻结 3=注销
   */
  val memberStatusUdf = udf { (status: Int) => {
    status match {
      case 0 => "未激活"
      case 1 => "激活"
      case 2 => "冻结"
      case 3 => "注销"
      case _ => status.toString
    }
  }
  }

}
