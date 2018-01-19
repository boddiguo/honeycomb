package com.boddi.honeycomb.sparkbee.util;

import java.util.Vector;

/**
 * Created by guoyubo on 2018/1/3.
 */
abstract class Node {
  // all nodes need data, parent, and a capacity
  protected Vector<DataNode> data;
  protected Node parent;
  protected int maxsize;

  public boolean isLeafNode() {
    // determine if a node is a leafnode
    return this.getClass().getName().trim().equals("LeafNode");
  }

  // both types of node need to insert and search
  abstract Node insert(DataNode dnode);
  abstract boolean search(DataNode x);

  protected boolean isFull() {
    return data.size() == maxsize-1;
  }

  public DataNode getDataAt(int index) {
    return (DataNode) data.elementAt(index);
  }

  protected void propagate(DataNode dnode, Node right) {
    // propogate takes a piece of data and two pointers left(this) and right and pushes the data up the tree

    // if there was no parent
    if(parent == null) {

      // create a new parent
      TreeNode newparent = new TreeNode(maxsize);

      // add the necessary data and pointers
      newparent.data.add(dnode);
      newparent.pointer.add(this);
      newparent.pointer.add(right);

      // update the parent information for right and left
      this.setParent(newparent);
      right.setParent(newparent);
    }
    else {
      // if the parent is not full
      if( ! parent.isFull() ) {
        // add the necessary data and pointers to existing parent
        boolean dnodeinserted = false;
        for(int i = 0; !dnodeinserted && i < parent.data.size(); i++) {
          if( ((DataNode)parent.data.elementAt(i)).inOrder(dnode) ) {
            parent.data.add(i,dnode);
            ((TreeNode)parent).pointer.add(i+1, right);
            dnodeinserted = true;
          }
        }
        if(!dnodeinserted) {
          parent.data.add(dnode);
          ((TreeNode)parent).pointer.add(right);
        }

        // set the necessary parent on the right node, left.parent is already set
        right.setParent(this.parent);
      }
      // the parent is full
      else {
        // split will take car of setting the parent of both nodes, because
        // there are 3 different ways the parents need to be set
        ((TreeNode)parent).split(dnode, this, right);

      }
    }
  }

  public int size() {
    return data.size();
  }

  @SuppressWarnings("unchecked") Node(int degree) {
    // initially the parent node is null
    parent = null;

    data = new Vector();
    maxsize = degree;
  }

  // Convert a node to a string
  public String toString() {
    String s = "";
    for(int i=0; i < data.size(); i++) {
      s += ((DataNode)data.elementAt(i)).toString() + " ";
    }
    return s + "#";
  }

  // this operation traverses the tree using the parent nodes until the parent is null and returns the node
  protected Node findRoot() {
    Node node = this;

    while(node.parent != null) {
      node = node.parent;
    }

    return node;
  }

  protected void setParent(Node newparent) {
    this.parent = newparent;
  }
}
