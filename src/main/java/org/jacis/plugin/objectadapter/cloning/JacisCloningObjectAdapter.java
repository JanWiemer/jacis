/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.plugin.objectadapter.cloning.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.objectadapter.cloning.readonly.JacisStoreEntryReadOnlyModeAdapter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JacisCloningObjectAdapter<V> implements JacisObjectAdapter<V, V> {

  private final JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter;

  public JacisCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    this.readOnlyModeAdapter = readOnlyModeAdapters;
  }

  public JacisCloningObjectAdapter() {
    this(new DefaultJacisStoreEntryReadOnlyModeAdapter<V>());
  }

  @Override
  public V cloneCommitted2WritableTxView(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneValue(value);
    return readOnlyModeAdapter.switchToReadWriteMode(clone);
  }

  @Override
  public V cloneCommitted2ReadOnlyTxView(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneValue(value);
    return clone;
  }

  @Override
  public V cloneTxView2Committed(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneValue(value);
    return readOnlyModeAdapter.switchToReadOnlyMode(clone);
  }

  @Override
  public V cloneTxView2ReadOnlyTxView(V value) {
    if (value == null || readOnlyModeAdapter.isReadOnly(value)) {
      return value;
    }
    V clone = cloneValue(value);
    return readOnlyModeAdapter.switchToReadOnlyMode(clone);
  }

  @SuppressWarnings("unchecked")
  private V cloneValue(V value) {
    if (value == null) {
      return null;
    }
    V clone;
    if (value instanceof JacisCloneable) {
      clone = ((JacisCloneable<V>) value).clone();
    } else {
      clone = cloneByReflection(value);
    }
    return clone;
  }

  @SuppressWarnings("unchecked")
  private V cloneByReflection(V obj) {
    try {
      Method cloneMethod = obj.getClass().getDeclaredMethod("clone");
      return (V) cloneMethod.invoke(obj);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! No clone method declared: " + e, e);
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    }
  }

}
