/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.testhelper;

import java.util.List;

import org.jacis.trackedviews.TrackedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test tracked view...
 *
 * @author Jan Wiemer
 */
public class TrackedTestView implements TrackedView<TestObject> {

  private static final Logger log = LoggerFactory.getLogger(TestObject.class);

  private int count = 0;
  private long sum = 0;

  @SuppressWarnings("unchecked")
  @Override
  public TrackedView<TestObject> clone() {
    try {
      return (TrackedView<TestObject>) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("clone murks");
    }
  }

  public int getCount() {
    return count;
  }

  public long getSum() {
    return sum;
  }

  @Override
  public void clear() {
    count = 0;
    sum = 0;
  }

  @Override
  public void trackModification(TestObject oldValue, TestObject newValue) {
    log.trace("VIEW: tack modification {} -> {}  --> {}", oldValue, newValue, Thread.currentThread().getName());
    sum += newValue == null ? 0 : newValue.getValue();
    sum -= oldValue == null ? 0 : oldValue.getValue();
    if (oldValue == null && newValue != null) {
      count++;
    } else if (oldValue != null && newValue == null) {
      count--;
    }
  }

  @Override
  public void checkView(List<TestObject> values) {
    if (count != values.stream().filter(v -> v != null).count()) {
      throw new RuntimeException("View expects " + count + " values but we have: " + values.size() + "! Values: " + values);
    }
    long checkSum = values.stream().mapToLong(v -> v == null ? 0 : v.getValue()).sum();
    if (sum != checkSum) {
      throw new RuntimeException("View expects sum " + sum + " values but we have: " + checkSum + "! Values: " + values);
    }
  }

}
