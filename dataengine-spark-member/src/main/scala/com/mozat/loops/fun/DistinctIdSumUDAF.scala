package com.dataengine.spark.fun

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * create by roy 2020-03-18
  * id去重，并求合
  *
  */
class DistinctIdSumUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    new StructType()
      //      .add("barnd", StringType)
      .add("orderid", StringType)
      .add("sum_v", DoubleType)

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    println("update==>>>", buffer, input) //餐品 有多少个订单
    val orderid = input.getAs[String](0)
    val sum_v = input.getAs[Double](1)
    //取出新加入的行，并加入缓存区
//    buffer(0) = buffer.getSeq[String](0) ++ orderid
    buffer(0) =  Seq[String](orderid)
    buffer(1) = buffer.getAs[Double](1) + sum_v

  }

  override def bufferSchema: StructType = {
    //    new StructType().add("items", ArrayType(new StructType().add("orderid", StringType).add("sumv", DoubleType), true), nullable = true)
    new StructType().add("orderid", ArrayType(StringType)).add("sumv", DoubleType)
  }

  //合并数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//    println("merge==>", buffer1, "|", buffer2)
    val b1Seq = buffer1.getSeq[String](0)
    val orderid=buffer2.getSeq[String](0)(0)//第1列，第1个元素
    if (!b1Seq.contains(orderid)) {
      buffer1(0) = b1Seq ++ orderid
      buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    }
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq[String]()
    buffer(1) = 0.0

  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    //    buffer.getSeq[String](0).length
    buffer.getAs[Double](1)
  }

  override def dataType: DataType = DoubleType

  case class ObjetValus(orderId: String, sumV: Double)

}