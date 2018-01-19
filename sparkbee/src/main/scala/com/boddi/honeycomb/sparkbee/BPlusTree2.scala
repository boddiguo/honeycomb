package com.boddi.honeycomb.sparkbee

import com.boddi.honeycomb.sparkbee.util.LeafNode

import scala.collection.mutable.ListBuffer

/**
  * Created by guoyubo on 2018/1/4.
  */


case class DataNode2(value:Integer)

class BPlusTree2(degree:Int) {

  var tree:Node2 = new LeafNode2(degree)


  def addNode (value: Integer): Unit = {
    tree = tree.insert(value)
  }

  override def toString: String = {
    tree.toString
  }
}


abstract class Node2(degree:Int) {

  var parent:TreeNode2 = null

  def insert(value: Integer): Node2


}

case class TreeNode2(degree:Int) extends Node2(degree) {

  var data:ListBuffer[DataNode2] = ListBuffer()

  var pointer:ListBuffer[Node2] = ListBuffer()

  override def insert(value: Integer): Node2 =  {

    return null
  }
}

case class LeafNode2(degree:Int) extends Node2(degree) {

  var data:ListBuffer[DataNode2] = ListBuffer()

  var next:LeafNode2 = null

  override def insert(value: Integer): Node2 =  {

    var node = new DataNode2(value)

    var added = false
    var i = 0
    while (
      !added && i < data.size
    ) {
      if (data(i).value >= value) {
        data.insert(i, node)
        added = true
      } else {
        i += 1
      }
    }
    if (!added)
      data += node
    if (data.size > degree) {
      // calculate the split point; ceiling(degree/2)
      var splitlocation = 0
      if (degree % 2 == 0) splitlocation = degree / 2
      else splitlocation = (degree + 1) / 2

      val mid = data(splitlocation)

      // create new LeafNode
      val right = new LeafNode2(degree)
      for(i <- 0 until data.size - splitlocation) {
        right.data += data.remove(splitlocation)
      }
      this.next = right

      this.propagate(mid, right)

    }
    return null

  }

  protected def propagate(dnode: DataNode2, right: Node2): Unit = {

  }




}