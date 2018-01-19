package com.boddi.honeycomb.sparkbee.util;

import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.Arrays;
    import java.util.Collections;
    import java.util.Comparator;
    import java.util.List;
    import java.util.stream.Collectors;


    import com.google.common.collect.Lists;

    import lombok.Data;
    import lombok.RequiredArgsConstructor;

/**
 * @author hongtao
 * @version v 0.1 , 2017年12月26日 上午9:59:22
 * @since JDK 1.8
 */
public class TestFunc {


  static String funcReturnStr(String inputArrayStr) {
    return func(Arrays.asList(inputArrayStr.substring(1, inputArrayStr.length() - 1).split(","))
                      .stream().map(str -> Pair.fromString(str)).collect(Collectors.toList())).toString()
                                                                                              .replaceAll(" ", "");
  }

  /**
   * (1,3),(4,5),(2,4),(4,6),(8,9) -> [(1,6)],[(8,9)]
   */
  static List<Pair> func(List<Pair> input) {
    List<Pair> output = Lists.newArrayList();
    Collections.sort(input, Comparator.comparingLong(Pair::getEndTime).reversed()); // 根据endTime从大到小排序
    // (8,9),(4,6),(4,5),(2,4),(1,3)
    int maxIndex = input.size() - 1;
    int currentIndex = 0;
    Pair mergedNode = null;
    while (currentIndex <= maxIndex) {
      Pair p = input.get(currentIndex);
      if (mergedNode == null) { // 第一个节点,自合并
        mergedNode = p;
      } else if (mergedNode.getStartTime() > p.getEndTime()) { // 无交集,加入输出
        output.add(mergedNode);
        mergedNode = p;
      } else { // 有交集, 合并节点
        mergedNode = new Pair(Math.min(mergedNode.getStartTime(), p.getStartTime()),
            Math.max(mergedNode.getEndTime(), p.getEndTime()));
      }
      if (currentIndex == maxIndex) { // 最后一个节点,无论合并与否加入输出
        output.add(mergedNode);
      }
      currentIndex++;
    }
    return output;
  }

  @Data
  @RequiredArgsConstructor
  public static class Pair {
    private final Long startTime;
    private final Long endTime;

    public Pair(final long startTime, final long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public static Pair fromString(String str) {
      String[] arr = str.substring(1, str.length() - 1).split("-");
      return new Pair(Long.valueOf(arr[0]), Long.valueOf(arr[1]));
    }

    public String toString() {
      return String.format("(%s-%s)", startTime, endTime);
    }
  }

  public static void main(String[] args) {

    StopWatch stopWatch = StopWatch.createStarted();
//(3,1),(4,5),(2,4),(4,6),(8,9)
    List<Pair> data = new ArrayList<>();
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
//    List<Pair<Integer>> sort = new SortDemo<Integer>(data).sort();

    List<TestFunc.Pair> sort = TestFunc.func(data);
    System.out.println(sort);

    stopWatch.stop();
    System.out.println(stopWatch.getTime());

  }

}
