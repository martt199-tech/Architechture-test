package org.marta.app.Processes

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.DefaultHttpClient

import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson
import org.apache.http.entity.StringEntity
import org.apache.http.params.HttpConnectionParams
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, get_json_object, to_json}

import scala.io.Source.fromInputStream

object HttpApi{
  def HttpJsonPost(dataFrame: DataFrame) = {

    // create our object as a json string

    val post = {
      val httpPost = new HttpPost("https://my-cool-api-endpoint")
      httpPost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      val json_str = """{"text": [""" + dataFrame.rdd.map(x => x.getAs[String]("json")) + "]}"
      httpPost.setEntity(new StringEntity(json_str))
      httpPost
    }

  }
  //Esto nos daraia el stiring correspondiente al texto, asumiendo que en la url iria el id del dato que se requiere
  def getRestContent(url: String,
                     connectionTimeout: Int,
                     socketTimeout: Int): String = {
    val httpClient = buildHttpClient(connectionTimeout, socketTimeout)
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent
      content = fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager.shutdown
    content
  }
  private def buildHttpClient(connectionTimeout: Int, socketTimeout: Int):
  DefaultHttpClient = {
    val httpClient = new DefaultHttpClient
    val httpParams = httpClient.getParams
    HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout)
    HttpConnectionParams.setSoTimeout(httpParams, socketTimeout)
    httpClient.setParams(httpParams)
    httpClient
  }
}
