package com.dataengine.spark.core.utils

import com.dataengine.spark.core.constant.DataDBConstant
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, struct, udf, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.parsing.json.JSONObject

/**
  * create roy by 2019/09/19
  * spark utils
  */
object SparkUtils {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) instance = SparkSession
      .builder
      .enableHiveSupport()
      .master("yarn")
      .appName("SparkSessionSingleton")
      .config(sparkConf)
      .getOrCreate()
    instance
  }

  def tableExists(table: String, spark: SparkSession) =
    spark.catalog.tableExists(table)

  def getValuesMapExt[T](row: Row, schema: StructType): Map[String, Any] = schema.fields.map {
    field =>
      try if (field.dataType.typeName.equals("struct")) field.name -> getValuesMapExt(row.getAs[Row](field.name), field.dataType.asInstanceOf[StructType]) else field.name -> row.getAs[T](field.name) catch {
        case e: Exception => {
          field.name -> null.asInstanceOf[T]
        }
      }
  }.filter(xy => xy._2 != null).toMap

  val addExtColUDF = udf {
    (r: Row) => {
      println("r==", getValuesMapExt(r, r.schema))
      JSONObject(getValuesMapExt(r, r.schema)).toString()
      //      JSONObject(r.getValuesMap(r.schema.fieldNames)).toString()
    }
  }

  /**
    * 1.比较两个DF,真实数据源DF比hive表的DF多的列转成json string 并存入ext_col列中
    * 2.出现非基本类型的转成json字符串
    *
    * @param hiveDataFrame
    * @param dataDataFrame
    * @return
    */
  def compareSchema(hiveDataFrame: DataFrame, dataDataFrame: DataFrame): DataFrame = {
    var newDataDataFrame = dataDataFrame
    val fieldNameDifferenceSet = dataDataFrame.schema.fieldNames.toSet -- hiveDataFrame.schema.fieldNames.toSet
    if (fieldNameDifferenceSet.size > 0) newDataDataFrame = newDataDataFrame.withColumn(DataDBConstant.cd_ext_col, to_json(struct(fieldNameDifferenceSet.toSeq.map(name => col(name)): _*))).drop(fieldNameDifferenceSet.toList: _*)
    newDataDataFrame.schema.foreach(structname => {
      if (structname.dataType.simpleString.startsWith("array")) newDataDataFrame = newDataDataFrame.withColumn(structname.name, to_json(col(structname.name))) else if (structname.dataType.simpleString.startsWith("map")) newDataDataFrame = newDataDataFrame.withColumn(structname.name, to_json(col(structname.name))) //map( else if (structname.dataType.simpleString.startsWith("struct")) newDataDataFrame = newDataDataFrame.withColumn(structname.name, to_json(col(structname.name)))
    })
    if (!newDataDataFrame.columns.contains(DataDBConstant.cd_ext_col)) newDataDataFrame = newDataDataFrame.withColumn(DataDBConstant.cd_ext_col, lit(null) cast "string")
    val fieldNameDifferenceSet2 = hiveDataFrame.schema.fieldNames.toSet -- newDataDataFrame.schema.fieldNames.toSet
    if (fieldNameDifferenceSet2.size > 0) {
      val addNullCol = hiveDataFrame.select(fieldNameDifferenceSet2.toSeq.map(name => col(name)): _*)
      addNullCol.schema.foreach(col => {
        newDataDataFrame = newDataDataFrame.withColumn(col.name, lit(null) cast col.dataType)
      })
    }
    newDataDataFrame
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val jsonString ="""{"k":"kkk","v":100,"t":"cc","t1":"bb","animal_interpretation":{"is_large_animal":"aaa","is_mammal":"dddd"},"arrtest":[{"status":7,"df":"ff"}]}"""
    //    val jsonString ="""[{"k":"kkk","v":100,"b":"bb","b2":100.55}]"""
    val jsonDF = spark.sqlContext.read.json(Seq(jsonString).toDS)
    jsonDF.printSchema()
    jsonDF.show()
    val jsonString2 ="""{"k":"kkk","v":100,"b":"bb","b2":100.55,"arrtest":[{"status":7,"df":"ff"}]}"""
    val jsonDF2 = spark.sqlContext.read.json(Seq(jsonString2).toDS)
    jsonDF2.show()
    //    val tyes = jsonDF.schema.fields.map(f => f.dataType)
    //    jsonDF2.withColumn("t1", lit("")).show()
    val cccDF = compareSchema(jsonDF2, jsonDF)
    cccDF.printSchema()
    cccDF.show(false)
  }
}
