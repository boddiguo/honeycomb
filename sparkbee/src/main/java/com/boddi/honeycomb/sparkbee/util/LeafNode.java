package com.boddi.honeycomb.sparkbee.util;

/**
 * Created by guoyubo on 2018/1/3.
 */
class LeafNode extends Node {
  private LeafNode nextNode;

  LeafNode(int degree) {
    super(degree);

    // initially the nextnode is null
    nextNode = null;
  }

  private void setNextNode(LeafNode next) {
    nextNode = next;
  }

  protected LeafNode getNextNode() {
    return nextNode;
  }

  public boolean search(DataNode x) {
    // search through the data sequentially until x is found, or there are no more entries
    for(int i=0; i < data.size(); i++) {
      if( ((DataNode)data.elementAt(i)).getData() == x.getData() ) {
        return true;
      }
    }
    return false;
  }

  protected void split(DataNode dnode) {
    // insert dnode into the vector (it will now be overpacked)
    boolean dnodeinserted = false;
    for(int i=0; !dnodeinserted && i < data.size(); i++) {
      if( ((DataNode)data.elementAt(i)).inOrder(dnode) ) {
        data.add(i,dnode);
        dnodeinserted = true;
      }
    }
    if(!dnodeinserted) {
      data.add(data.size(), dnode);
    }

    // calculate the split point; ceiling(maxsize/2)
    int splitlocation;
    if(maxsize%2 == 0) {
      splitlocation = maxsize/2;
    }
    else {
      splitlocation = (maxsize+1)/2;
    }

    // create new LeafNode
    LeafNode right = new LeafNode(maxsize);

    for(int i = data.size()-splitlocation; i > 0; i--) {
      right.data.add(data.remove(splitlocation));
    }

    // link the two together
    right.setNextNode(this.getNextNode());
    this.setNextNode(right);

    // get the middle item's data
    DataNode mid =  (DataNode) data.elementAt(data.size()-1);

    // propagate the data and pointers into the parent node
    this.propagate(mid, right);
  }

  public Node insert(DataNode dnode) {
    // if the leaf isn't full insert it at the proper place
    if(data.size() < maxsize-1) {
      boolean dnodeinserted = false;
      int i = 0;
      while(!dnodeinserted && i < data.size()) {
        if( ((DataNode)data.elementAt(i)).inOrder(dnode) ) {
          data.add(i,dnode);
          dnodeinserted = true;
        }
        i++;
      }
      if(!dnodeinserted) {
        data.add(data.size(), dnode);
      }
    }

    // if the leaf is full split
    else {
      this.split(dnode);
    }

    // return the root of the tree
    return this.findRoot();
  }
}
