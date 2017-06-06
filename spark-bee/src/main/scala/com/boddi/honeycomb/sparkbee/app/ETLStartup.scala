package com.boddi.honeycomb.sparkbee.app


import java.text.SimpleDateFormat
import java.util.Date

import com.boddi.honeycomb.sparkbee.entity.TableConfig
import com.boddi.honeycomb.sparkbee.repository.TableConfigRepository
import com.boddi.honeycomb.sparkbee.service.{SparkExtractor, SparkLoader}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
  * Created by guoyubo on 2017/5/11.
  */
object ETLStartup {

  private val MYSQL_CONNECTION_URL = "jdbc:mysql://127.0.0.1:3306/etl_config"
  private val MYSQL_USERNAME = "root"
  private val MYSQL_PWD = "123456"
  case class Jndi(driver:String, url:String, user:String, password:String)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark ETl Sample")
      .config("spark.master","local[*]")
      .getOrCreate()


//    val  tableConfDF = spark.read.format("jdbc").options(
//      Map("url"->MYSQL_CONNECTION_URL,
//        "dbtable"->s"(select id,BATCH_ID,FROM_TABLE_NAME,TABLE_OWNER,PRIMARY_KEY,TO_TABLE_NAME,MERGE_TYPE,JNDI_NAME from ETL_TABLE_CONF  ) as table_conf",
//        "driver"->"com.mysql.jdbc.Driver",
//        "user"-> MYSQL_USERNAME,
//        //"partitionColumn"->"day_id",
//        "lowerBound"->"0",
//        "upperBound"-> "1000",
//        //"numPartitions"->"2",
//        "fetchSize"->"100",
//        "password"->MYSQL_PWD)).load()
//    val result = tableConfDF.select("id","BATCH_ID","FROM_TABLE_NAME")
//    tableConfDF.show(100)

//    val  jndiConfDF = spark.read.format("jdbc").options(
//      Map("url"->MYSQL_CONNECTION_URL,
//        "dbtable"->"(select jndi_name,driver,url,user,password from ETL_JNDI_CONF) as jndi_conf",
//        "driver"->"com.mysql.jdbc.Driver",
//        "user"-> MYSQL_USERNAME,
//        //"partitionColumn"->"day_id",
//        "lowerBound"->"0",
//        "upperBound"-> "1000",
//        //"numPartitions"->"2",
//        "fetchSize"->"100",
//        "password"->MYSQL_PWD)).load()


    val jdbcConfDF = spark.read.json("/Users/guoyubo/workspace/honeycomb/spark-bee/src/main/resources/jdbc_conf.json")
    jdbcConfDF.show()
    jdbcConfDF.createOrReplaceTempView("ETL_JNDI_CONF")

    val map = spark.sql("select jndi_name,driver,url,user,password from ETL_JNDI_CONF ").rdd.map(e=>(e.getString(0)->(e.getString(1),e.getString(2), e.getString(3), e.getString(4)))).cache()
    val jdniMap = map.collect().toMap

    val tableConfDF = spark.read.json("/Users/guoyubo/workspace/honeycomb/spark-bee/src/main/resources/table_conf.json")
    tableConfDF.show()
    tableConfDF.createOrReplaceTempView("ETL_TABLE_CONF")

    val extractor = new SparkExtractor(spark)
    val loader = new SparkLoader(spark)


    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    var dateStr = dateFormatter.format(new Date())
    val result = spark.sql("select BATCH_ID,FROM_TABLE_NAME,TABLE_OWNER,PRIMARY_KEY,TO_TABLE_NAME,MERGE_TYPE,JNDI_NAME from ETL_TABLE_CONF ").rdd.collect().foreach(e => {
      val jdni = jdniMap(e.getString(6))
      val tablePath = "./odl/" + e.getString(4)
      extractor.execute("jdbc", jdni._1, jdni._2, jdni._3, jdni._4,
        e.getString(1), e.getString(3), "1=1", e.getString(4), tablePath, dateStr)
      loader.execute(e.getString(4), "")
    })

    spark.stop()
  }

}
