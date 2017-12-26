package com.boddi.honeycomb.sparkbee.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.language.implicitConversions


/**
  * Created by guoyubo on 2017/12/20.
  */
object DataPipelineStream {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataPipelineStream")
    val streamingContext = new StreamingContext(conf, Seconds(10))
    val sqlContext = new SQLContext(streamingContext.sparkContext)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("words")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    val map = stream.flatMap(record => record.value().split("\n")).map {
//      value =>
//      try {
//        JsonUtil.fromJson[EventModel](value)
//      } catch {
//        case ex:Exception => println(ex)
//      }
//    }
//    map.print()
//
//    stream.foreachRDD(jsonRDD =>{
//        val data = sqlContext.read.json(jsonRDD.map(_.value()))
//        data.createOrReplaceTempView("event_data")
//        sqlContext.sql("SELECT * FROM event_data").show()
//
//      })

    streamingContext.checkpoint("checkpoint")
    val words = stream.flatMap(record => record.value().split("\n"))      // DStream transformation
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(30), 2)
    words.print()

    case class EventModel(name:String, sender:String, data:JSONObject) extends Serializable {
      override def toString: String = JsonUtil.toJson(this)
    }

    val map = words.map(
      value =>
        try {
          val jsonObject  = JSON.parseObject(value)
          new EventModel(jsonObject.getString("name"), jsonObject.getString("sender"), jsonObject.getJSONObject("data"))
      } catch {
        case ex:Exception => println(ex)
        null
      }
    )
    map.print()

    val configContent = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("event_metics_conf.json")).mkString

    val eventMetricsConfigs:List[EventMetricsConfig] = EventMetricsConfig.parseConfig(configContent)

    for (elem:EventMetricsConfig <- eventMetricsConfigs) {
      val metricsList:List[MetricsConfig] = elem.getMetricsList
      println(elem.name)
      println(elem.windowSize)
      val metricsStream = map.filter(_.name==elem.name)//.window(Seconds(elem.windowSize), Seconds(elem.windowSize));
      for (metrics: MetricsConfig <- metricsList) {
        println(metrics.metricsType)
        streamingContext.checkpoint("checkpoint")

        if (metrics.metricsType == "Counter") {
//          val counter = metricsStream.countByWindow(Seconds(elem.windowSize), Seconds(elem.windowSize))
          val counter = metricsStream.map(e => (metrics.name, 1L)).reduceByKey(_+_)
          counter.print()
        } else if (metrics.metricsType == "Gauge") {

          val func = getAggregationFunc(metrics.getConfigByKey("aggregationFuncs"))
          val aggregationFieldStream = metricsStream.map(e => (metrics.name, e.data.getDouble(metrics.getConfigByKey("aggregationField")) + 0.00))
          if (metrics.getConfigByKey("aggregationFuncs") == "mean") {
            def createComb = (v:Double) => (1, v)

            def mergeVal:((Int,Double),Double)=>(Int,Double) =
            {case((c,s),v) => (c+1, s+v)}

            def mergeComb:((Int,Double),(Int,Double))=>(Int,Double) =
            {case((c1,s1),(c2,s2)) => (c1+c2, s1+s2)}

            val avgrdd = aggregationFieldStream.combineByKey(createComb, mergeVal, mergeComb,
                new org.apache.spark.HashPartitioner(2))
              .mapValues({case(x,y)=>(x, y, y/x)})
            avgrdd.print()
          } else {
            val reduceStream = aggregationFieldStream.reduceByKey(func)
            reduceStream.print()
          }
        } else if (metrics.metricsType == "Histogram") {

          var dimentionFiled = "None"
          if (metrics.getConfigByKey("aggregationField") != null) {
            if (metrics.getConfigByKey("dimentionFields") != null) {
              dimentionFiled = metrics.getConfigByKey("dimentionFields")
            }
            val aggregationFieldStream = metricsStream.map(e => (metrics.name+"_"+dimentionFiled, e.data.getDouble(metrics.getConfigByKey("aggregationField")) + 0.00))
            aggregationFieldStream.groupByKey().map((k) => {
              val list = k._2.toList.sortWith(_ < _)
              val vcount = list.size
              val vsum = list.sum
              val vmean = 1.0 * vsum / vcount
              val vmax = list(vcount - 1)
              val vmin = list(0)
              val p50 = list(( (vcount-1)*0.5 ).round.toInt)
              val p75 = list(( (vcount-1)*0.75 ).round.toInt)
              val p95 = list(( (vcount-1)*0.95 ).round.toInt)
              val p99 = list(( (vcount-1)*0.99 ).round.toInt)
              val p999 = list(( (vcount-1)*0.999 ).round.toInt)

              (k._1, (vcount, vsum, vmean, vmax, vmin, p50, p75, p95, p99, p999))
            }).print()


          }
        }

//        val metricsMap:DStream[(String, Long)] = map.filter(_.name==elem.name).
//          map(e => ("Metrics", e.data.getLong("cnt")))
//        metricsMap.reduceByKeyAndWindow(_+_, _-_, Seconds(elem.windowSize), Seconds(elem.windowSize)).print()
      }

    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }




  def getAggregationFunc(func:String) = (x:Double, y:Double) => {
    if (func == "sum") {
      (x + y)
    } else if (func == "min") {
      math.min(x, y)
    } else if (func == "max") {
      math.max(x, y)
    }  else {
      0.00
    }
  }


}
