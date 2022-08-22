package org.marta.app.Processes

import org.apache.spark.sql.functions.{col, count, max}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.marta.app.Main.conf

import scala.util.Try

object ProcessWriter {

  def readFromHive(sparkSession: SparkSession, tableName: String): DataFrame = {

    sparkSession.read.table(tableName)
  }


  def addSaga(dataFrame: DataFrame): DataFrame = {
    val url = Try(conf.getConfig("api").getString("getUri")).getOrElse()
    val connectionTimeout = Try(conf.getConfig("api").getInt("connectionTimeout")).get
    val socketTimeout = Try(conf.getConfig("api").getInt("csocketTimeout")).get

    dataFrame.withColumn("saga", col( HttpApi.getRestContent(s"${url}/${col("id")}",connectionTimeout,socketTimeout)))
  }


  def calculateMetrics(df: DataFrame): DataFrame = {
    val initialTimestamp = Try(conf.getConfig("filtersTime").getString("initialTimestamp")).getOrElse(None)
    val finalTimestamp =Try(conf.getConfig("filtersTime").getString("finalTimestamp")).getOrElse(None)
    df.filter(col("initialTimestamp").cast("timestamp") > initialTimestamp and(
      col("finalTimestamp").cast("timestamp")< finalTimestamp
    ))
      .select(
      col("saga"), col("numLectores"),

    )
      .orderBy("numLectores")
      .groupBy("saga")
      .agg(count("numLectores"))

  }
}
