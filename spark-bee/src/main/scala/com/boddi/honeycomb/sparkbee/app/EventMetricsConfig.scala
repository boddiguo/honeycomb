package com.boddi.honeycomb.sparkbee.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/**
  * Created by guoyubo on 2017/12/22.
  */

class EventMetricsConfig(val name:String, val sender:String, val windowSize:Long, val metrics:JSONArray) extends Serializable {


  def this(config:JSONObject) = {
    this(config.getString("name"), config.getString("sender"), config.getLong("windowSize"), config.getJSONArray("metrics"))
  }

  def getMetricsList:List[MetricsConfig] = {
    var metricsConfig = new ListBuffer[MetricsConfig]()
    for (i <- 0 until metrics.size()) {
      val metricsConfigObj = new MetricsConfig(metrics.getJSONObject(i))
      metricsConfig += metricsConfigObj
    }
    metricsConfig.toList


//    for (elem:JSONObject <- metrics.) {
//      metricsConfig.::(new MetricsConfig(elem))
//    }
//    metricsConfig
  }

}

class MetricsConfig (val name:String, val metricsType:String, val config:JSONObject) extends Serializable {

  def this(metrics:JSONObject) = {
    this(metrics.getString("name"), metrics.getString("type"),  metrics.getJSONObject("config"))
  }

  def getConfigByKey(key:String) = {
    config.getString(key)
  }


}

object EventMetricsConfig {

  def parseConfig(config:String): List[EventMetricsConfig] = {
    val jsonConfig = JSON.parseArray(config)
    val configs =  new ListBuffer[EventMetricsConfig]()
//    for (elem:JSONObject <- jsonConfig) {
//      configs.::(new EventMetricsConfig(elem))
//    }
//    configs

    for (i <- 0 until jsonConfig.size()) {
      println(jsonConfig.getJSONObject(i))
      val configObj = new EventMetricsConfig(jsonConfig.getJSONObject(i))
      configs += configObj
    }
    return configs.toList
  }

  def main(args: Array[String]): Unit = {
    val configContent = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("event_metics_conf.json")).mkString

    val eventMetricsConfigs:List[EventMetricsConfig] = EventMetricsConfig.parseConfig(configContent)
    println(eventMetricsConfigs.size)
  }

}
