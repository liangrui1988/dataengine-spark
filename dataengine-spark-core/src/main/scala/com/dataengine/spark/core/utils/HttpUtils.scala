package com.dataengine.spark.core.utils

import java.net.{HttpURLConnection, URL}
import java.util

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.http.{Consts, NameValuePair}

/**
  * Created by Roy on 2018/11/20
  * http请求
  */
object HttpUtils {
  //  @throws(classOf[java.io.IOException])
  //  @throws(classOf[java.net.SocketTimeoutException])
  def get(url: String,
          connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET"): String = {
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val content = scala.io.Source.fromInputStream(connection.getInputStream, "utf-8").mkString
    content
  }

  def doGet(url: String): String = try get(url) catch {
    case _: java.io.IOException => null // handle this
    case _: java.net.SocketTimeoutException => null // handle this
  }

  def doPost(url: String, data: Map[String, Any]): String = {
    val httpClients = HttpClients.createDefault
    val entity = new UrlEncodedFormEntity(createParam(data), Consts.UTF_8)
    val post = new HttpPost(url)
    post.setEntity(entity)
    val response = httpClients.execute(post)
    val html = EntityUtils.toString(response.getEntity, "UTF-8")
    EntityUtils.consume(response.getEntity)
    html
  }

  def sendGet(url: String): String = try {
    val content = get(url)
    content
  } catch {
    case ioe: java.io.IOException => "" // handle this
    case ste: java.net.SocketTimeoutException => "" // handle this
  }

  def createParam(param: Map[String, Any]): util.ArrayList[NameValuePair] = {
    val nvps = new util.ArrayList[NameValuePair]
    for (l <- param.toList) {
      nvps.add(new BasicNameValuePair(l._1, l._2.toString))
    }
    nvps
  }

  def main(args: Array[String]): Unit = {
    val html = doPost("http://httpbin.org/post", Map("hello" -> "world"))
    println(html)
  }
}
