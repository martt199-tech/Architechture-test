package org.marta.app.config

import com.typesafe.config.ConfigFactory

trait ConfigProperties {
  val conf = ConfigFactory.load()
}
