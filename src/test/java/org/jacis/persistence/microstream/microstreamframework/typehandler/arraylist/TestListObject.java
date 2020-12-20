/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.persistence.microstream.microstreamframework.typehandler.arraylist;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestListObject {

  private final List<String> content;
  private final Set<Integer> modified;

  public TestListObject(int initialCapacity) {
    content = new ArrayList<>(initialCapacity);
    modified = new HashSet<>();
  }

  public List<String> getContent() {
    return content;
  }

  public Set<Integer> getModified() {
    return modified;
  }

  public TestListObject add(String element) {
    content.add(element);
    modified.add(content.size() - 1);
    return this;
  }

  public TestListObject set(int idx, String element) {
    content.set(idx, element);
    modified.add(idx);
    return this;
  }

  public TestListObject remove(int idx) {
    content.set(idx, content.get(content.size() - 1));
    content.remove(content.size() - 1);
    modified.add(idx);
    modified.add(content.size());
    return this;
  }

  @Override
  public String toString() {
    return content.toString();
  }

}
