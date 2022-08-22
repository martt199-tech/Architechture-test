package org.marta.app

import org.apache.spark.sql.SparkSession
import org.marta.app.Processes.{KafkaReader, ProcessWriter}
import org.marta.app.config.ConfigProperties

import scala.util.Try
object Main extends App with ConfigProperties {
  val spark = SparkSession.builder()
    .appName("DataLoader")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.getConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
  spark.sparkContext.getConf.set("hive.metastore.uris", "thrift://localhost:9083")


 //Read Conf file
      // READ STREAM FROM KAFKA
      KafkaReader.writeRaw(spark)
     val resultDf =  ProcessWriter.readFromHive(spark,conf.getConfig("hive").getString("tablaRaw"))
      val dfToWrite = ProcessWriter.calculateMetrics(resultDf)
    dfToWrite.write.saveAsTable(s"${conf.getConfig("hive").getString("tablaBusiness")}")
    // WRITE IN HIVE


  }
