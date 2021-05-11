package com.dataengine.spark.core.app

import com.dataengine.spark.core.utils.HttpUtils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Set

/**
  * Created by Roy on 2018/11/20
  * 节假日任务处理 <p>
  * 一般本年12月份公布下年的假日，如果没有传时间，默认当年,一个月跑一次全年的,重复覆盖
  *
  * #args {year-2018} default Current Year
  * spark-submit --class wmdataETLs.FestivalData --master yarn-cluster --executor-cores 2 --num-executors 6 --driver-memory 4G --executor-memory 2G --name festivalData /home/spark/sparkjars/bijobs/bigcandao_2.11-0.1.jar  2016
  */
object FestivalData {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) = {
    var startTime = 0
    // 开始年
    var endTime = 0 //节束年
    if (args.size >= 1) {
      startTime = args(0).toInt
      if (args.size >= 2) {
        endTime = args(1).toInt
      } else {
        endTime = startTime
      }
      println(args.size + " =args size   " + startTime + endTime)
    }
    //如果没有参数，取当年
    if (startTime == 0) {
      import java.util.Calendar
      val cal = Calendar.getInstance
      // cal.add(Calendar.YEAR, 1)
      val c_year = cal.get(Calendar.YEAR)
      startTime = c_year
      endTime = c_year
    }
    //循环每年,每个月
    logger.info("FestivalData start " + startTime + " - " + endTime)
    var allListResultSet: Set[DtTable] = Set()
    for (a_time <- startTime to endTime) {
      // for 循环12个月的数据
      for (month <- 1 to 12) {
        val yearMonth = a_time + "-" + month
        println("Value of yearMonth: " + yearMonth)
        try {
          val listResult = getMonthFestivalData(yearMonth)
          allListResultSet = allListResultSet ++ listResult
        } catch {
          case e: Exception => println(e.getMessage)
        }
      }
    }
    // -----------------------------------------
    // 开始写入到hive库
    /// -----------------------------------------
    //    val spark = SparkSession.builder().appName("BiWeatherInfo") //.master("local[*]") .config("hive.metastore.uris", "thrift://hdp02:9083")
    //      .enableHiveSupport().getOrCreate()

    //先把数据全查出来，去重后，再写回
    //    spark.sql(" use bi ")
    //    try {
    //      import spark.implicits._
    //      val dbData = spark.sql("select dt,add_time,dt_type,festival_name from festival_info")
    //      val dbData2 = dbData.map(attributes => DtTable(attributes(0).toString, attributes(1).toString, attributes(2).toString.toInt, attributes(3).toString)).collect().toSet
    //      allListResultSet = allListResultSet ++ dbData2
    //    } catch {
    //      case ex: Exception => {
    //        println("Exception -> festival_info Table or view not found")
    //      }
    //    }
    //    val caseClassDS = spark.createDataFrame(allListResultSet.toSeq).toDF("dt", "add_time", "dt_type", "festival_name")
    //    //去重
    //    caseClassDS.write.mode(SaveMode.Overwrite).saveAsTable("bi.festival_info")
    // spark.close()
  }

  /**
    * 根据月时间，获取当月的所有节假日
    *
    * @param yearMonth
    */
  def getMonthFestivalData(yearMonth: String): Set[DtTable] = {
    val listResult: Set[DtTable] = Set()
    val url = "http://v.juhe.cn/calendar/month?key=f882e0be5c0624e3b2752fba31180646&year-month=" + yearMonth
    val result = HttpUtils.sendGet(url)
    // println(url)
    println("result>>>>" + result)
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = org.json4s.DefaultFormats
    val pjson = parse(result).extract[Map[String, Any]]
    if (pjson.get("error_code").get.toString.toInt == 0) {
      //日期
      val dateFormate: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
      val c_date = dateFormate.format(new Date())
      val ls = pjson.get("result").get.asInstanceOf[Map[String, Any]].get("data").get.asInstanceOf[Map[String, Any]].get("holiday").get.toString
      //println("ls>>>>" + ls)
      val l2 = parse(ls).extract[List[Map[String, Any]]]
      case class PList(date: String, status: Int)
      l2.foreach(x => {
        val festivalName = x.get("name").get.toString
        //节假日
        val list_map = x.get("list").get.asInstanceOf[List[Map[String, Any]]]
        list_map.foreach(y => {
          // println(y.get("date").get.toString + "   " + y.get("status").get.toString.toInt)
          // if (y.get("status").get.toString.toInt == 1) {
          val date = y.get("date").get.toString
          //节日
          //val dateFormate: SimpleDateFormat = new SimpleDateFormat("yyyy-M-d")
          val MM_dd = new SimpleDateFormat("yyyy-MM-dd", Locale.US).format(new SimpleDateFormat("yyyy-M-d").parse(date))
          val status = y.get("status").get.toString.toInt //状态，1=节假日，2=原周末，但上班
          var festivalNameNew = festivalName
          if (status == 2) {
            festivalNameNew = festivalNameNew + "-补班"
          }
          listResult.add(new DtTable(MM_dd, c_date, status, festivalNameNew))
          // }
        })
      })
    } else {
      logger.error("请求异常：{}", pjson.toString())
    }
    listResult
  }

  case class DtTable(dt: String, add_time: String, dt_type: Int, festival_name: String) {
    override def hashCode(): Int = return dt.hashCode

    //override def equals(obj: Any): Boolean = this.dt==obj.toString.toInt
    override def equals(obj: Any): Boolean = return this.dt == obj.asInstanceOf[DtTable].dt
  }

}
