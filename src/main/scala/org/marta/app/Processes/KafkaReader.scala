package org.marta.app.Processes

import com.twitter.finagle.http.Response
import com.typesafe.config.Config
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.unix_timestamp
import org.marta.app.Main.conf
import org.marta.app.config.ConfigProperties

import scala.util.Try

object KafkaReader extends ConfigProperties{

  def writeRaw(sparkSession: SparkSession) = {
    val kafkaServer = Try(conf.getConfig("kafka").getString("server"))
    val topic = Try(conf.getConfig("kafka").getString("topic"))
    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer.get)
      .option("subscribe", conf.getString(topic.get))
      .option("startingOffsets", "earliest") // From starting
      .load()

    val hiveDatabase = Try(conf.getConfig("hive").getString("databaseRaw"))
    val hiveTable= Try(conf.getConfig("hive").getString("tableRaw"))
    //Esto escribiria en Hive, en la tabla entitiesRaw
    df.withColumn("DATE", unix_timestamp())
      .write.partitionBy("DATE").saveAsTable(s"${hiveDatabase.get}.${hiveTable.get}")

  }

   //ESta funcion haria una peticion get con cada registro del dframe(id_articulo y texto)
    // y devolveria un strring, con la saga a la que corresponde cada articulo
  }

  /*
  * La tabla que se escribiria en Hive sería la siguiente:
  * * id_articulo
  * * readers: cuanta gente leyo el id_articulo
  * * saga : Despues de pasar por el proceso de clasificacion
  * */

