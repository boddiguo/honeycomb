package com.boddi.honeycomb.sparkbee.service

import org.apache.spark.sql.SparkSession

/**
  * Created by guoyubo on 2017/5/15.
  */
class HiveTemplate(sparkSession: SparkSession) {

  def execute(sql:String): Unit = {
    sparkSession.sql(sql)
  }

}
