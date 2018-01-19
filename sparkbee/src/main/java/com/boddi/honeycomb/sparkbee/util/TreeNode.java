package com.boddi.honeycomb.sparkbee.util;

import java.util.Vector;

/**
 * Created by guoyubo on 2018/1/3.
 */
class TreeNode extends Node {
  protected Vector<Node> pointer;

  // constructor for TreeNode
  // x-1 is the maximum # of DataNodes a single node can store
  @SuppressWarnings("unchecked")
  TreeNode(int x) {
    super(x);
    pointer = new Vector();
  }

  // this will find the correct pointer to the next lowest level of the tree where x should be found
  public Node getPointerTo(DataNode x) {
    // find the index i where x would be located
    int i = 0;
    boolean xptrfound = false;
    while(!xptrfound && i < data.size()) {
      if( ((DataNode)data.elementAt(i)).inOrder(x ) ) {
        xptrfound = true;
      }
      else {
        i++;
      }

    }


    // return the Node in pointer(i)
    return (Node) pointer.elementAt(i);

  }

  // returns the pointer at a specific index in the pointer stack
  public Node getPointerAt(int index) {
    return (Node) pointer.elementAt(index);
  }

  boolean search(DataNode dnode) {
    // get a pointer to where dnode.data should be found
    Node next = this.getPointerTo(dnode);

    // recursive call to find dnode.data if it is present
    return next.search(dnode);
  }

  protected void split(DataNode dnode, Node left, Node right) {
    // calculate the split point ( floor(maxsize/2)
    int splitlocation, insertlocation = 0;
    if(maxsize%2 == 0) {
      splitlocation = maxsize/2;
    }
    else {
      splitlocation = (maxsize+1)/2 -1;
    }

    // insert dnode into the vector (it will now be overpacked)
    boolean dnodeinserted = false;
    for(int i=0; !dnodeinserted && i < data.size(); i++) {
      if( ((DataNode)data.elementAt(i)).inOrder(dnode) ) {
        data.add(i,dnode);
        ((TreeNode)this).pointer.remove(i);
        ((TreeNode)this).pointer.add(i, left);
        ((TreeNode)this).pointer.add(i+1, right);
        dnodeinserted = true;

        // set the location of the insert this will be used to set the parent
        insertlocation = i;
      }
    }
    if(!dnodeinserted) {
      // set the location of the insert this will be used to set the parent
      insertlocation = data.size();
      data.add(dnode);
      ((TreeNode)this).pointer.remove(((TreeNode)this).pointer.size()-1);
      ((TreeNode)this).pointer.add(left);
      ((TreeNode)this).pointer.add(right);

    }

    // get the middle dataNode
    DataNode mid = (DataNode) data.remove(splitlocation);

    // create a new tree node to accomodate the split
    TreeNode newright = new TreeNode(maxsize);

    // populate the data and pointers of the new right node
    for(int i=data.size()-splitlocation; i > 0; i--) {
      newright.data.add(this.data.remove(splitlocation));
      newright.pointer.add(this.pointer.remove(splitlocation+1));
    }
    newright.pointer.add(this.pointer.remove(splitlocation+1));

    // set the parents of right and left
    // if the item was inserted before the split point both nodes are children of left
    if(insertlocation < splitlocation) {
      left.setParent(this);
      right.setParent(this);
    }
    // if the item was inserted at the splitpoint the nodes have different parents this and right
    else if(insertlocation == splitlocation) {
      left.setParent(this);
      right.setParent(newright);
    }
    // if the item was was inserted past the splitpoint the nodes are children of right
    else {
      left.setParent(newright);
      right.setParent(newright);
    }

    // propogate the node up
    this.propagate(mid, newright);
  }

  Node insert(DataNode dnode) {
    Node next = this.getPointerTo(dnode);

    return next.insert(dnode);
  }
}
