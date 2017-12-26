package com.boddi.honeycomb.sparkbee.app

import org.apache.spark._
import org.apache.spark.streaming._
/**
  * Created by guoyubo on 2017/12/1.
  */
object SparkStream {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val textStream = ssc.socketTextStream("localhost", 9000)
    val words = textStream.flatMap(_.split(" "))

//    val counts = words.map(e => (e, 1)).reduceByKey(_+_)
    val counts = words.map(e => (e, 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(30))
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
