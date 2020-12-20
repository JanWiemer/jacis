/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.persistence.microstream.microstreamframework.typehandler.linkedlist;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestLinkedListObject<T> {

  private transient final List<TestLinkedListEntry<T>> content;
  private transient final Set<TestLinkedListEntry<T>> modified;
  private TestLinkedListEntry<T> head = null;

  public TestLinkedListObject() {
    content = new ArrayList<>();
    modified = new HashSet<>();
  }

  public int getSize() {
    return content.size();
  }

  public TestLinkedListEntry<T> getHead() {
    return head;
  }

  void restoreContent() {
    TestLinkedListEntry<T> e = head;
    while (e != null) {
      content.add(e);
      e = e.getSucc();
    }
  }

  public TestLinkedListObject<T> add(T element) {
    return add(getSize() - 1, element); // add at the end
  }

  public TestLinkedListObject<T> add(int idx, T element) {
    TestLinkedListEntry<T> oldEntry = content.get(idx);
    TestLinkedListEntry<T> predEntry = oldEntry.getPred();
    TestLinkedListEntry<T> newEntry = new TestLinkedListEntry<T>(element, predEntry, oldEntry);
    if (predEntry != null) {
      predEntry.setSucc(newEntry);
    } else {
      head = newEntry;
    }
    if (oldEntry != null) {
      oldEntry.setPred(newEntry);
    }
    trackModified(predEntry);
    trackModified(newEntry);
    trackModified(oldEntry);
    content.add(idx, newEntry);
    return this;
  }

  public TestLinkedListObject<T> set(int idx, T element) {
    TestLinkedListEntry<T> entry = content.get(idx);
    entry.setObject(element);
    trackModified(entry);
    return this;
  }

  public TestLinkedListObject<T> remove(int idx) {
    int size = getSize();
    TestLinkedListEntry<T> oldEntry = content.get(idx);
    TestLinkedListEntry<T> lastEntry = content.get(size);
    if (idx < size) {
      set(idx, lastEntry.getObject());
    }
    TestLinkedListEntry<T> predOfLast = lastEntry.getPred();
    if (predOfLast != null) {
      predOfLast.setSucc(null);
    } else {
      head = null;
    }
    content.set(idx, content.get(content.size() - 1));
    content.remove(content.size() - 1);
    trackModified(oldEntry);
    trackModified(lastEntry);
    trackModified(predOfLast);
    return this;
  }

  protected void trackModified(TestLinkedListEntry<T> entry) {
    if (entry != null) {
      modified.add(entry);
    }
  }

  @Override
  public String toString() {
    return content.toString();
  }

}

class TestLinkedListEntry<T> {

  private T object;
  private TestLinkedListEntry<T> pred;
  private TestLinkedListEntry<T> succ;

  public TestLinkedListEntry(T object, TestLinkedListEntry<T> pred, TestLinkedListEntry<T> succ) {
    this.object = object;
    this.pred = pred;
    this.succ = succ;
  }

  public T getObject() {
    return object;
  }

  public void setObject(T object) {
    this.object = object;
  }

  public TestLinkedListEntry<T> getPred() {
    return pred;
  }

  public void setPred(TestLinkedListEntry<T> pred) {
    this.pred = pred;
  }

  public TestLinkedListEntry<T> getSucc() {
    return succ;
  }

  public void setSucc(TestLinkedListEntry<T> succ) {
    this.succ = succ;
  }

  @Override
  public String toString() {
    return object.toString();
  }

}
