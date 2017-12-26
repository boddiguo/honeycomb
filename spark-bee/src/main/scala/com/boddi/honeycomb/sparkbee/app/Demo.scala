package com.boddi.honeycomb.sparkbee.app

import org.apache.spark.{SparkConf, SparkContext}

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
