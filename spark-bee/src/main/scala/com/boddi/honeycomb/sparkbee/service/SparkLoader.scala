package com.boddi.honeycomb.sparkbee.service

import java.io.IOException

import org.apache.spark.sql.SparkSession
import scala.reflect.io.{File, Path}

/**
  * Created by guoyubo on 2017/6/5.
  */
class SparkLoader(sparkSession:SparkSession) extends java.io.Serializable {

  def execute(tableName:String, updatedAt:String): Unit = {
    sparkSession.read.parquet("./odl/"+tableName).createOrReplaceTempView(tableName)
    sparkSession.sql(s"select * from (SELECT d.*,row_number() over(partition by dw_key order by dw_updated_at desc) rn " +
      s"from  $tableName d WHERE DATA_DATE>'2017-06-05 11:39') d where d.rn=1")
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet("./odl/"+tableName+"_inc")

    sparkSession.read.parquet("./odl/"+tableName+"_inc").createOrReplaceTempView(tableName+"_inc")
    val path = Path("./idl/"+tableName)
    if (path.exists) {
      sparkSession.read.parquet("./idl/"+tableName).createOrReplaceTempView(tableName+"_idl")
      sparkSession.sql(" SELECT  * FROM " + tableName + "_inc " +
        "UNION ALL    " +
        "SELECT TGT.* FROM  ( SELECT  *  FROM  " + tableName + "_idl) TGT  " +
        "LEFT JOIN "  + tableName + "_inc SRC " +
        "ON    (TGT.dw_key=SRC.dw_key)    " +
        "WHERE    SRC.dw_key IS NULL").write
        .mode(org.apache.spark.sql.SaveMode.Overwrite).parquet("./idl/"+tableName+"_temp")
      sparkSession.read.parquet("./idl/"+tableName+"_temp").createOrReplaceTempView(tableName+"_idl_tmp")

      sparkSession.sql("select * from "+ tableName + "_idl_tmp").
        write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet("./idl/"+tableName)
    } else {
      sparkSession.sql(s"select * from "+tableName+"_inc").write.parquet("./idl/"+tableName)

    }

    sparkSession.read.parquet("./idl/"+tableName).createOrReplaceTempView(tableName+"_idl")
    sparkSession.sql("select count(*) from "+ tableName+"_idl").foreach(println(_))
    sparkSession.sql("select count(*) from "+ tableName+"_inc").foreach(println(_))
  }
}
