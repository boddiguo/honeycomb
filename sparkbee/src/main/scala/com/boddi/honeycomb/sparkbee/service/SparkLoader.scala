package com.boddi.honeycomb.sparkbee.service

import java.io.IOException

import com.boddi.honeycomb.sparkbee.util.HadoopClient
import org.apache.spark.sql.SparkSession

import scala.reflect.io.{File, Path}

/**
  * Created by guoyubo on 2017/6/5.
  */
class SparkLoader(sparkSession:SparkSession) extends java.io.Serializable {

  def execute(tableName:String, updatedAt:String, hdfsPath:String, dataStr:String): Unit = {
//    val filePath = hdfsPath + "/odl/" + tableName + "/" + dataStr
    val filePath = hdfsPath + "/odl/" + tableName + "/" + dataStr
    if (!HadoopClient.exist(filePath)) {
      HadoopClient.mkdirs(filePath)
    }
    sparkSession.read.parquet(filePath).createOrReplaceTempView(tableName)
    val incPath = hdfsPath + "/odl/" + tableName + "_inc"
    sparkSession.sql(s"select * from (SELECT d.*,row_number() over(partition by dw_key order by dw_updated_at desc) rn " +
      s"from  $tableName d WHERE DATA_DATE>'2017-06-05 11:39') d where d.rn=1")
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(incPath)

    sparkSession.read.parquet(incPath).createOrReplaceTempView(tableName+"_inc")
    val idlPath = hdfsPath + "/idl/" + tableName
    if (Path(idlPath).exists) {
      sparkSession.read.parquet(idlPath).createOrReplaceTempView(tableName+"_idl")
      sparkSession.sql(s"""SELECT  * FROM  $tableName + "_inc
        UNION ALL
        SELECT TGT.* FROM  ( SELECT  *  FROM  " + $tableName + "_idl) TGT
        LEFT JOIN "  + $tableName + "_inc SRC
        ON    (TGT.dw_key=SRC.dw_key)
        WHERE    SRC.dw_key IS NULL""").write
        .mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(hdfsPath + "/idl/"+tableName+"_temp")
      sparkSession.read.parquet(hdfsPath + "/idl/"+tableName+"_temp").createOrReplaceTempView(tableName+"_idl_tmp")

      sparkSession.sql("select * from "+ tableName + "_idl_tmp").
        write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(idlPath)
    } else {
      sparkSession.sql(s"select * from "+tableName+"_inc").write.parquet(idlPath)
    }

    sparkSession.read.parquet(idlPath).createOrReplaceTempView(tableName+"_idl")
    sparkSession.sql("select count(*) from "+ tableName+"_idl").foreach(println(_))
    sparkSession.sql("select count(*) from "+ tableName+"_inc").foreach(println(_))
  }
}
