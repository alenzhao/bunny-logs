package com.google.bunnylog.analyzer.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Input: a collection of intervals, sorted by left coordinate.
 * Output: for each interval, an integer "Y" coordinate such that
 *   intervals at the same "Y" coordinate don't overlap
 *   (they may touch, though)
 */
public class IntervalLayout {

  public static List<Integer> computeYCoordinate(List<OperationInterval> intervals) {
    ArrayList<Integer> answer = new ArrayList(intervals.size());
    ArrayList<Date> ends = new ArrayList<Date>(0);
    for (OperationInterval i : intervals) {
      int ok = firstFree(ends, i.getStart());
      if (ok<0) {
        ok = ends.size();
        ends.add(i.getEnd());
      } else {
        ends.set(ok, i.getEnd());
      }
      answer.add(ok);
    }
    return answer;
  }

  private static int firstFree(ArrayList<Date> ends, Date now) {
    int index=0;
    for (Date l : ends) {
      if (l.compareTo(now)<0) return index;
      index++;
    }
    return -1;
  }

}

