package com.dataengine.spark.core.utils

import java.util.Properties

import org.apache.spark.sql.catalog.Database

/**
  * create by roy 2019/09/10
  */
object ResourcesUtils {


  def main(args: Array[String]): Unit = {

    println("ddd")
  }


  def getPropValues(user: String = "user", passwd: String = "passwd", url: String = "url", filename: String = "jdbc.properties"): (String, String, String) = {
    val prop = getProperties(filename)
    (prop.getProperty(user), prop.getProperty(passwd), prop.getProperty(url))
  }

  def getloopsActivityPropValues(): (String, String, String,String,String) = {
    val prop = getProperties("jdbc.properties")
    (prop.getProperty("new_loops_activity_user"), prop.getProperty("new_loops_activity_password"),
      prop.getProperty("new_loops_activity_jdbcUrl"), prop.getProperty("new_loops_activity_host"), prop.getProperty("new_loops_activity_prot"))
  }




  def getProperties(filename: String = "jdbc.properties"): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(filename)
    prop.load(inputStream)
    prop
  }

  def getMogodbPropValues(url: String = "ds6uri", database: String = "ds6database"): (String, String) = {
    val prop = getProperties("mongo.properties")
    (prop.getProperty(url), prop.getProperty(database))
  }

  def getSettings(): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("settings.properties")
    prop.load(inputStream)
    prop
  }

  def getEnv(key: String = "env") = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("env.conf")
    prop.load(inputStream)
    prop.getProperty(key, "prod")
  }

}
