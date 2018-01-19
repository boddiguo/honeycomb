package com.boddi.honeycomb.sparkbee.util;

import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by guoyubo on 2017/12/27.
 */
public class SortDemo<T extends Comparable> {

  private final List<Pair<T>> data;

  public SortDemo(final List<Pair<T>> data) {
    this.data = data;
  }

  public static class Pair<T extends Comparable> {
    public T start;
    public T end;

    public Pair(final T start, final T end) {
      this.start = start;
      this.end = end;
    }

    public T getStart() {
      return start;
    }

    public void setStart(final T start) {
      this.start = start;
    }

    public T getEnd() {
      return end;
    }

    public void setEnd(final T end) {
      this.end = end;
    }

    @Override
    public String toString() {
      return "Pair{" +
          "start=" + start +
          ", end=" + end +
          '}';
    }
  }

  public List<Pair<T>> sort() {
    List<Pair<T>> mergeList = new ArrayList<>();
    for (Pair<T> pair : data) {
      System.out.println("start handle " + pair);
      if (mergeList.isEmpty()) {
        mergeList.add(pair);
      } else {
        Integer index = null;
        List<Integer> removalIndexs = new ArrayList<>();
        mergeit:for (int i = 0; i < mergeList.size(); i++) {
          Pair<T> mergePair = mergeList.get(i);
          if (pair.start.compareTo(mergePair.end) > 0) {
            continue;
          } else if (pair.end.compareTo(mergePair.start) < 0){
            index = i;
            break mergeit;
          } else {
            mergePair.setStart(pair.start.compareTo(mergePair.start) > 0 ? mergePair.start : pair.start);
            mergePair.setEnd(pair.end.compareTo(mergePair.end) > 0 ? pair.end : mergePair.end);

            int j = i+1;
            mergew:while(j < mergeList.size()) {
              if (mergeList.get(j).start.compareTo(mergePair.end) <= 0) {
                  mergePair.setEnd(mergeList.get(j).end.compareTo(mergePair.end) > 0 ? mergeList.get(j).end : mergePair.end);
                  removalIndexs.add(j++);
                  i++;
              } else {
                break mergeit;
              }
            }
          }
        }

        for (int removalIndex : removalIndexs) {
          mergeList.remove(removalIndex);
        }
        if (index != null) {
          mergeList.add(index, pair);
        }
        if (pair.start.compareTo(mergeList.get(mergeList.size()-1).end) > 0){
          mergeList.add(pair);
        }
      }

    }
    return mergeList;

  }


  public static void main(String[] args) {

    StopWatch stopWatch = StopWatch.createStarted();
//(3,1),(4,5),(2,4),(4,6),(8,9)
    List<Pair<Long>> data = new ArrayList<>();
    int i = 0;
    while (i < 100000) {
      data.add(new Pair(1, 3));
      data.add(new Pair(4, 5));
      data.add(new Pair(2, 4));
      data.add(new Pair(4, 6));
      data.add(new Pair(2, 9));
      data.add(new Pair(8, 9));
      data.add(new Pair(14, 19));
      data.add(new Pair(8, 9));
      data.add(new Pair(9, 10));
      data.add(new Pair(1, 2));
      data.add(new Pair(13, 14));
      data.add(new Pair(12, 20));
      data.add(new Pair(123, 233));
      data.add(new Pair(40, 233));
      data.add(new Pair(20, 40));
      i++;
    }
    List<Pair<Long>> sort = new SortDemo<Long>(data).sort();

    System.out.println(sort);

    stopWatch.stop();
    System.out.println(stopWatch.getTime());

  }
}
