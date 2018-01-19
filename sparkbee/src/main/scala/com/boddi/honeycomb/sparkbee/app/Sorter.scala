package com.boddi.honeycomb.sparkbee.app

/**
  * Created by guoyubo on 2018/1/9.
  */

object Sorter {


  def insertSort(arr:Array[Int]):Array[Int] = {
    var i = 0
    for (i<-1 until arr.length) {
      var temp = arr(i)
      var j = i
      var finded = false
      while (j >= 0 && !finded){
        if (temp.compareTo(arr(j - 1)) < 0) {
          arr(j) = arr(j - 1)
          j -= 1
        } else {
          arr(j) = temp
          finded = true
        }
      }

    }
    arr
  }

  def bubbleSort(arr:Array[Int]):Array[Int] = {
    var i = arr.length-1

    while (i > 0) {

      var j = 0
      while (j < i) {
        if (arr(j).compareTo(arr(j + 1)) > 0) {
          val tmp = arr(j)
          arr(j) = arr(j + 1)
          arr(j + 1) = tmp
        }
        j += 1
      }

      i -= 1
    }
    arr
  }


  def selectSort(arr:Array[Int]):Array[Int] = {
    for (i<-0 until arr.length - 1) {
      var tmp = arr(i)
      var minIndex = i
      for (j<-i+1 until arr.length) {
        if (arr(j).compareTo(tmp) < 0) {
          tmp = arr(j)
          minIndex = j
        }
      }
      if (minIndex != i) {
        arr(minIndex) = arr(i)
        arr(i) = tmp
      }
    }
    arr
  }






  def main(args: Array[String]): Unit = {
    println(Sorter.insertSort(Array(1,34,44,5,56,8,8,523,1,3,3,56,7)).mkString(" "))
    println(Sorter.bubbleSort(Array(1,34,44,5,56,8,8,523,1,3,3,56,7)).mkString(" "))
    println(Sorter.selectSort(Array(1,34,44,5,56,8,8,523,1,3,3,56,7)).mkString(" "))
  }



}
