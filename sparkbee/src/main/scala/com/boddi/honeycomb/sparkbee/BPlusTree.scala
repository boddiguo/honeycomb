package com.boddi.honeycomb.sparkbee

import javafx.scene.Parent

import com.boddi.honeycomb.sparkbee.BPlusTree.printNode

import scala.collection.mutable.ListBuffer

/**
  * Created by guoyubo on 2018/1/3.
  */
class BPlusTree(degree:Int) {


  var tree:Node = new LeafNode(degree)


  def addNode (value: Int): Unit = {
    println(s"start add value $value")

    tree = tree.insert(value)
  }

  def search(value: Int):Boolean = {
    println(s"start search value $value")
    tree.search(value)
  }

  def deleteNode(value: Int):Boolean = {
    println(s"start delete value $value")

    tree.delete(value)
  }

  override def toString: String = {
    tree.toString
  }
}

abstract class Node(degree:Int) {

  var parent:TreeNode = null

  var data: ListBuffer[DataNode] = new ListBuffer[DataNode]

  def insert(value: Int): Node

  def delete(value: Int): Boolean

  def search(value: Int): Boolean

  // this operation traverses the tree using the parent nodes until the parent is null and returns the node
  protected def findRoot: Node = {
    var node = this
    while (node.parent != null) node = node.parent
    node
  }

  protected def isFull: Boolean = data.size == degree - 1


  protected def propagate(dnode: DataNode, right: Node): Unit = {
    // propogate takes a piece of data and two pointers left(this) and right and pushes the data up the tree
    // if there was no parent
    if (parent == null) { // create a new parent
      val newparent = new TreeNode(degree)
      // add the necessary data and pointers
      newparent.data += dnode
      newparent.pointer += this
      newparent.pointer += right
      // update the parent information for right and left
      this.parent = newparent
      right.parent = newparent
    } else { // if the parent is not full
      if (!parent.isFull) { // add the necessary data and pointers to existing parent

        var dnodeinserted = false
        var i = 0
        while (
          !dnodeinserted && i < parent.data.size
        ) {
          if (parent.data(i).value >= dnode.value) {
            parent.data.insert(i, dnode)
            parent.pointer.insert(i+1, right)
            dnodeinserted = true
          } else {
            i += 1
          }
        }
        if (!dnodeinserted) {
          parent.data += dnode
          parent.pointer += right
        }

        // set the necessary parent on the right node, left.parent is already set
        right.parent = this.parent
      }
      else { // the parent is full
        // split will take car of setting the parent of both nodes, because
        // there are 3 different ways the parents need to be set
        parent.split(dnode, this, right)
      }
    }
  }

  protected def propagateMerge(mergeNode: Node): Unit = {

    if (mergeNode.parent != null && mergeNode.parent.data.size < degree - 1) {


    }
  }


  override def toString: String =  {
    this.data.toString
  }


}

case class TreeNode(degree:Int) extends Node(degree) {

  var pointer: ListBuffer[Node] = new ListBuffer[Node]

  override def insert(value: Int): Node = {

    var dnodeinserted = false
    var i = 0
    while (
      !dnodeinserted && i < data.size
    ) {
      if (data(i).value >= value) {
        dnodeinserted = true
      } else {
        i += 1
      }
    }

    return pointer(i).insert(value)
  }

  def split(dnode: DataNode, left: Node, right: Node): Unit = {
    // calculate the split point ( floor(maxsize/2)
    var splitlocation: Int = 0
    var insertlocation: Int = 0
    if (degree % 2 == 0) splitlocation = degree / 2
    else splitlocation = (degree + 1) / 2 - 1
    // insert dnode into the vector (it will now be overpacked)

    var dnodeinserted = false
    var i = 0
    while (
      !dnodeinserted && i < data.size
    ) {
      if (data(i).value >= dnode.value) {
        data.insert(i, dnode)
        this.pointer.remove(i)
        pointer.insert(i, left)
        pointer.insert(i + 1, right)
        dnodeinserted = true
        // set the location of the insert this will be used to set the parent
        insertlocation = i
      } else {
        i += 1
      }
    }
    if (!dnodeinserted) {
      insertlocation = data.size

      data += dnode
      this.pointer.remove(this.pointer.size -1)
      this.pointer += left
      this.pointer += right
    }

    // get the middle dataNode
    val mid = data.remove(splitlocation)
    // create a new tree node to accomodate the split
    val newright = new TreeNode(degree)
    // populate the data and pointers of the new right node

    for(i <- 0 until data.size - splitlocation) {
      newright.data += data.remove(splitlocation)
      newright.pointer += pointer.remove(splitlocation+1)
    }

    newright.pointer += this.pointer.remove(splitlocation + 1)
    // set the parents of right and left
    // if the item was inserted before the split point both nodes are children of left
    if (insertlocation < splitlocation) {
      left.parent = this
      right.parent = this
    }
    else { // if the item was inserted at the splitpoint the nodes have different parents this and right
      if (insertlocation == splitlocation) {
        left.parent = this
        right.parent = newright
      }
      else { // if the item was was inserted past the splitpoint the nodes are children of right
        left.parent = newright
        right.parent = newright
      }
    }
    // propogate the node up
    this.propagate(mid, newright)
  }


  override def search(value: Int):Boolean = { // get a pointer to where dnode.data should be found
    var dnode = new DataNode(value)
    // find the index i where x would be located
    var dnodeinserted = false
    var i = 0
    while (
      !dnodeinserted && i < data.size
    ) {
      if (data(i).value >= dnode.value) {
        dnodeinserted = true
      } else {
        i += 1
      }
    }

    // recursive call to find dnode.data if it is present
    pointer(i).search(value)
  }

  override def delete(value: Int): Boolean = {
    // find the index i where x would be located
    var dnodeinserted = false
    var i = 0
    while (
      !dnodeinserted && i < data.size
    ) {
      if (data(i).value > value) {
        dnodeinserted = true
      } else {
        i += 1
      }
    }

    // recursive call to find dnode.data if it is present
    pointer(i).delete(value)

  }

  override def hashCode(): Int =  {
    this.degree.hashCode() + data.hashCode() + pointer.hashCode()
  }

  override def equals(obj: scala.Any): Boolean =  obj match{
    case that: TreeNode => that.degree == this.degree && that.data == this.data && that.pointer == this.pointer
    case _ => false
  }
}

case class DataNode(value:Int) {
  override def hashCode(): Int = value.hashCode()

  override def equals(obj: scala.Any): Boolean =  obj match {
    case that: DataNode  => that.value == this.value
    case _ => false
  }
}

case class LeafNode(degree:Int) extends Node(degree) {

  var nextNode:LeafNode = null

  override def insert(value: Int): Node = {
    val node = new DataNode(value)
    if (data.size < degree -1) {

      var dnodeinserted = false
      var i = 0
      while (
        !dnodeinserted && i < data.size
      ) {
        if (data(i).value >= value) {
          data.insert(i, node)
          dnodeinserted = true
        } else {
          i += 1
        }
      }
      if (!dnodeinserted)
        data += node

    } else {
      this.split(node)
    }
    return findRoot
  }


  def split(node: DataNode) =  {

    var dnodeinserted = false
    var i = 0
    while (
      !dnodeinserted && i < data.size
    ) {
      if (data(i).value >= node.value) {
        data.insert(i, node)
        dnodeinserted = true
      } else {
        i += 1
      }
    }
    if (!dnodeinserted) {
      data += node
    }

    // calculate the split point; ceiling(maxsize/2)
    var splitlocation = 0
    if (degree % 2 == 0) splitlocation = degree / 2
    else splitlocation = (degree + 1) / 2
    val mid = data(splitlocation)

    val right = new LeafNode(degree)
    for(i <- 0 until data.size - splitlocation) {
      right.data += data.remove(splitlocation)
    }
    this.nextNode = right

    // propagate the data and pointers into the parent node
    this.propagate(mid, right)

  }

  override def search(x: Int): Boolean = { // search through the data sequentially until x is found, or there are no more entries

    return data.exists(p => p.value == x)
  }

  override def toString: String = {
     "leaf node data:" + data
  }

  override def delete(value: Int): Boolean = {
    var exist = false
    var i = 0
    while (
      !exist && i < data.size
    ) {
      if (data(i).value == value) {
        exist = true
      } else {
        i += 1
      }
    }
    if (exist) {
      data.remove(i)

      def treeNode(parent: TreeNode):Option[(TreeNode, Int)] = {
        var exist_p = false
        var j = 0
        while (
          !exist_p && j < parent.data.size
        ) {
          if (parent.data(j).value == value) {
            exist_p = true
          } else {
            j += 1
          }
        }
        if(exist_p) {
          return Some(parent, j);
        } else  if (parent.parent !=  null) {
          return treeNode(parent.parent)
        } else {
          None
        }
      }

      val tuple = treeNode(this.parent)
      if (tuple != None) {
        tuple.get._1.data(tuple.get._2) = data(i)
      }

      this.merge()
      return true
    }

    return false
  }

  def merge() = {

    var splitlocation = 0
    if (degree % 2 == 0) splitlocation = degree / 2
    else splitlocation = (degree + 1) / 2

    var mergeNode:LeafNode = null
    if (nextNode != null  && nextNode.data.size < degree-2) {
      mergeNode = nextNode
    } else {
      val pointer = this.parent.pointer
      var i = 0
      var node: Node = null
      while (mergeNode == null && i < pointer.size) {
        node = pointer(i)
        node match {
          case LeafNode(_) =>
            if (node.asInstanceOf[LeafNode].nextNode == this) {
              mergeNode = node.asInstanceOf[LeafNode]
            }
        }
        i += 1
      }
    }

    if (mergeNode != null && mergeNode.data.size < degree-2) {
      if (mergeNode.data(0).value >= this.data(this.data.size-1).value) {
        this.data = this.data ++ mergeNode.data
        if (mergeNode.nextNode != null) {
          this.nextNode = mergeNode.nextNode
        } else {
          this.nextNode = null
        }
        if (mergeNode.data.size - 1 < splitlocation) {
          mergeNode.parent.pointer -= mergeNode
          mergeNode.parent.data -= mergeNode.data(0)
        }

      }

      //          else {
      //            mergeNode.data = mergeNode.data ++ this.data
      //            if (this.nextNode != null) {
      //              mergeNode.nextNode = this.nextNode
      //            } else {
      //              mergeNode.nextNode = null
      //            }
      //
      //            if (mergeNode.data.size - 1 < splitlocation) {
      //              this.parent.pointer -= this
      //              this.parent.data -= this.data(0)
      //            }
      //
      //          }

      if (splitlocation < mergeNode.data.size - 1) {

        val mid = mergeNode.data(splitlocation)

        val right = new LeafNode(degree)
        for (i <- 0 until mergeNode.data.size - splitlocation) {
          right.data += mergeNode.data.remove(splitlocation)
        }
        mergeNode.nextNode = right
        this.propagateMerge(mergeNode)
      }

    }
  }


  override def hashCode(): Int =  {
    this.degree.hashCode() + data.hashCode() + nextNode.hashCode()
  }

  override def equals(obj: scala.Any): Boolean =  obj match {
    case that: LeafNode => that.degree == this.degree && that.data == this.data && that.nextNode == this.nextNode
    case _ => false
  }

}

object BPlusTree extends App {

  var tree = new   BPlusTree(5)
  tree.addNode(3)
  tree.addNode(2)
  tree.addNode(21)
  tree.addNode(1)
  tree.addNode(5)
  tree.addNode(6)
  tree.addNode(7)
  tree.addNode(8)
  tree.addNode(10)
  tree.addNode(2)
  tree.addNode(4)
  tree.addNode(9)
  tree.addNode(34)
  tree.addNode(30)
  tree.addNode(134)
  tree.addNode(67)
//  tree.addNode(76)
//  tree.addNode(23)
//  tree.addNode(2334)

  printNode(1, tree.tree)

  private def printNode(level:Int,node:Node): Unit = {
    node match {
      case TreeNode(_) => {
        println("level-" +level +" tree node: " + node.data)
        val pointer = node.asInstanceOf[TreeNode].pointer
        if (pointer != null) {
          pointer.foreach(printNode(level+1, _))
        }
      }
      case LeafNode(_) =>
        println("level-" + level +" leaf node: " + node.data)

    }
  }

  println(tree.search(9))
  println(tree.search(99))
  println(tree.search(134))
  println(tree.search(1340))
  println(tree.deleteNode(1340))
  println(tree.deleteNode(134))
  printNode(1, tree.tree)
  println(tree.deleteNode(21))
  printNode(1, tree.tree)
  println(tree.deleteNode(34))
  printNode(1, tree.tree)
  println(tree.deleteNode(67))
  printNode(1, tree.tree)
  println(tree.deleteNode(10))
  printNode(1, tree.tree)

}