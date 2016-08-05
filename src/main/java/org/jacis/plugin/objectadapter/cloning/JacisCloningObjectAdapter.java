/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.exception.ReadOnlyModeNotSupportedException;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.plugin.objectadapter.cloning.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.objectadapter.cloning.readonly.JacisStoreEntryReadOnlyModeAdapter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Generic implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter} copying the objects
 * to and from the transactional view by pragmatically clone the object.
 * If the object is an instance of the {@link JacisCloneable} interface the {@link JacisCloneable#clone()} method
 * declared in this interface is used to clone the object.
 * Otherwise the object may be cloneable (overwrites the {@link Object#clone()} methods)
 * but does not implement the {@link JacisCloneable} interface.
 * In this case the {@link Object#clone()} method is called by reflection.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public class JacisCloningObjectAdapter<V> implements JacisObjectAdapter<V, V> {

  /** */
  private final JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter;

  public JacisCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    this.readOnlyModeAdapter = readOnlyModeAdapters;
  }

  public JacisCloningObjectAdapter() {
    this(new DefaultJacisStoreEntryReadOnlyModeAdapter<>());
  }

  @Override
  public V cloneCommitted2WritableTxView(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneValue(value);
    if (readOnlyModeAdapter != null && readOnlyModeAdapter.isApplicableTo(clone)) {
      clone = readOnlyModeAdapter.switchToReadWriteMode(clone);
    }
    return clone;
  }

  @Override
  public V cloneCommitted2ReadOnlyTxView(V value) {
    checkReadOnlyModeSupported(value);
    if (value == null) {
      return null;
    }
    return value; // for read only objects we do not clone the returned object. -> skip call of: cloneValue(value);
  }

  @Override
  public V cloneTxView2Committed(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneValue(value);
    if (readOnlyModeAdapter != null && readOnlyModeAdapter.isApplicableTo(clone)) {
      clone = readOnlyModeAdapter.switchToReadOnlyMode(clone);
    }
    return clone;
  }

  @Override
  public V cloneTxView2ReadOnlyTxView(V value) {
    checkReadOnlyModeSupported(value);
    if (value == null || readOnlyModeAdapter.isReadOnly(value)) {
      return value;
    }
    V clone = cloneValue(value);
    return readOnlyModeAdapter.switchToReadOnlyMode(clone);
  }

  private void checkReadOnlyModeSupported(V value) {
    if (value != null && (readOnlyModeAdapter == null || !readOnlyModeAdapter.isApplicableTo(value))) {
      throw new ReadOnlyModeNotSupportedException("Object of class " + value.getClass().getName() + " not supporting read only mode! Object: " + value);
    }
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
    } catch (SecurityException | IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    }
  }

}
