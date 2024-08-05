package org.jacis.util;


import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashSet<E> implements Set<E> {
  private final Map<E, Object> map;
  private static final Object OBJ = new Object();

  public ConcurrentHashSet(int size) {
    this.map = new ConcurrentHashMap<>(size);
  }

  public ConcurrentHashSet() {
    this.map = new ConcurrentHashMap();
  }

  public int size() {
    return this.map.size();
  }

  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  public boolean contains(Object o) {
    return this.map.containsKey(o);
  }

  public Iterator<E> iterator() {
    return this.map.keySet().iterator();
  }

  public Object[] toArray() {
    return this.map.keySet().toArray();
  }

  public <T> T[] toArray(T[] a) {
    return this.map.keySet().toArray(a);
  }

  public boolean add(E e) {
    return this.map.put(e, OBJ) == null;
  }

  public boolean remove(Object o) {
    return this.map.remove(o) != null;
  }

  public boolean containsAll(Collection<?> c) {
    return this.map.keySet().containsAll(c);
  }

  public boolean addAll(Collection<? extends E> collection) {
    boolean changed = false;
    Iterator<? extends E> iter = collection.iterator();
    while (iter.hasNext()) {
      E e = iter.next();
      if (this.map.put(e, OBJ) == null) {
        changed = true;
      }
    }
    return changed;
  }

  public boolean retainAll(Collection<?> collection) {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(Collection<?> collection) {
    boolean changed = false;
    Iterator<?> iter = collection.iterator();
    while (iter.hasNext()) {
      Object e = iter.next();
      if (this.map.remove(e) != null) {
        changed = true;
      }
    }
    return changed;
  }

  public void clear() {
    this.map.clear();
  }
}
