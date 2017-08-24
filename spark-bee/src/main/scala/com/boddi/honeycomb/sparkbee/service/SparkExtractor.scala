package com.boddi.honeycomb.sparkbee.service

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Path


/**
  * Created by guoyubo on 2017/5/15.
  */
class SparkExtractor(sparkSession:SparkSession)   extends java.io.Serializable {


   def execute(dbType:String, driverClass:String, url:String, userName:String, password:String,
               fromTable:String, primaryKey:String, condition:String, toTable:String,
               hdfsPath:String, dataStr:String): String = {
     val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")
     var updatedAt = dateFormatter.format(new Date())
     val jdbcDF = sparkSession.read.format(dbType).options(
       Map("driver" -> driverClass,
         "url" -> url,
         "dbtable" -> fromTable,
         "lowerBound"->"0",
         "upperBound"-> "1000",
         "numPartitions"->"2",
         "fetchSize"->"100",
         "user" -> userName,
         "password" -> password)).load()
     jdbcDF.createOrReplaceTempView(toTable)
     val dataFrame = sparkSession.sql(s"select REVERSE(CAST($primaryKey as char(50))) as DW_KEY,'$updatedAt' as DW_UPDATED_AT,d.* from $fromTable d where $condition");
     val filePath = hdfsPath + "/odl/DATA_DATE=" + dataStr
     val path = Path(filePath)
     if (path.exists) {
       try {
         path.deleteRecursively()
       } catch {
         case e: IOException => // some file could not be deleted
       }
     }

     dataFrame.write.parquet(filePath)
     val parquet = sparkSession.read.option("mergeSchema", "true").parquet(hdfsPath)
     println(parquet.schema)
     hdfsPath
   }


}
