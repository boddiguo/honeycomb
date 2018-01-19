package com.boddi.honeycomb.sparkbee.app

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.runtime.Nothing$

/**
  * Created by guoyubo on 2017/12/21.
  */
object Demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
    val b = a.flatMap(x => 1 to x)
    println(b.collect().toList)
    println(a.sum())
    println(a.mean())
    println(a.max())
    println(a.count())


    val test: Int => (String, Int) = {e => ("input ", e)}
    println(test(1))


    //初始化测试数据
    val foo = sc.parallelize(List(Tuple2("a", 1), Tuple2("a", 3), Tuple2("b", 2), Tuple2("b", 8)));
    //这里需要用到combineByKey这个函数，需要了解的请google
    val results=foo.combineByKey(
      (v)=>(v,1),
      (acc:(Int,Int),v) =>(acc._1+v,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)
    ).map{case(key,value)=>(key,value._1/value._2.toDouble)}
    results.collect().foreach(println)

    println(getAggregationFunc("sum")(9,10))

    val sortResult = sort(List((1,2),(16,17),(300,1100), (8,9), (3,4), (5,7), (2,10), (13,19), (12,14),(2,4),(6,8),(18,200)))
    println(sortResult)

  }

  def sort(data: List[(Long, Long)]): List[(Long, Long)] = {
    var mergeResult = ListBuffer[(Long, Long)]()
    val sortedData = data.sortBy(_._1)
    println(sortedData)

    var mergePair:(Long, Long) = null

    for (pair <- sortedData) {
      if (mergePair == null) {
        mergePair = pair
      } else if (mergePair._2 <  pair._1) {
        mergeResult += mergePair
        mergePair = pair
      } else {
        mergePair = (Math.min(mergePair._1, pair._1), Math.max(mergePair._2, pair._2))
      }

    }
    mergeResult += mergePair

    mergeResult.toList
  }





  def getAggregationFunc(func:String) = (x:Double, y:Double) => {
    if (func == "sum") {
      x + y
    } else if (func == "min") {
      math.min(x, y)
    } else if (func == "max") {
      math.max(x, y)
    }
  }

}
