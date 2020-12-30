package org.jacis.store;

import java.io.Serializable;

import org.jacis.JacisApi;

@JacisApi
public class KeyValuePair<K, TV> implements Serializable {

  public static <K, TV> KeyValuePair<K, TV> of(K key, TV val) {
    return new KeyValuePair<>(key, val);
  }

  private static final long serialVersionUID = 1L;

  private final K key;
  private final TV val;

  public KeyValuePair(K first, TV second) {
    this.key = first;
    this.val = second;
  }

  public K getKey() {
    return key;
  }

  public TV getVal() {
    return val;
  }

  @Override
  public String toString() {
    return "(" + key + ", " + val + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((val == null) ? 0 : val.hashCode());
    return result;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    KeyValuePair other = (KeyValuePair) obj;
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (val == null) {
      if (other.val != null) {
        return false;
      }
    } else if (!val.equals(other.val)) {
      return false;
    }
    return true;
  }

}