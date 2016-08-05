/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.exception.ReadOnlyModeNotSupportedException;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

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
 * <p>
 * If a read only mode adapter is set (see {@link #readOnlyModeAdapter}) and this read only mode adapter is applicable
 * for the objects in the store this adapter is used to switch the objects between the read-only and read-write mode.
 * In this case the committed values are always stored in read-only mode. When cloned to a transactional view
 * the cloned values are switched to read-write mode. When the objects are cloned back to the committed view
 * they are switched back to the read only mode. With this feature it is possible to access a read only view
 * of an object from the outside. This read only view is *not* cloned from the committed value. Instead the committed
 * value itself is returned to the caller. This is possible since the object is in read only mode.
 * <p>
 * If the read only mode is not supported accessing a read only value returns an ordinary cloned object.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public class JacisCloningObjectAdapter<V> implements JacisObjectAdapter<V, V> {

  /**
   * Read only mode adapter used to switch objects from writable to read only mode if required and supported
   */
  private final JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter;
  /** Flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported.*/
  private boolean throwIfMissingReadOnlyModeDetected = false;

  /**
   * Create a cloning object adapter with the passed read only mode adapter.
   * @param readOnlyModeAdapters Adapter to switch an object between read-only and read-write mode (if supported).
   */
  @SuppressWarnings("WeakerAccess")
  public JacisCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    this.readOnlyModeAdapter = readOnlyModeAdapters;
  }

  /**
   * Create a cloning object adapter with a default read only mode adapter (see {@link DefaultJacisStoreEntryReadOnlyModeAdapter}).
   */
  public JacisCloningObjectAdapter() {
    this(new DefaultJacisStoreEntryReadOnlyModeAdapter<>());
  }


  /**
   * Set the flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported.
   * @param throwIfMissingReadOnlyModeDetected  flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported.
   * @return The current instance for method chaining
   */
  public JacisCloningObjectAdapter<V> setThrowIfMissingReadOnlyModeDetected(boolean throwIfMissingReadOnlyModeDetected) {
    this.throwIfMissingReadOnlyModeDetected = throwIfMissingReadOnlyModeDetected;
    return this;
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
  public V cloneCommitted2ReadOnlyTxView(V value) {
    if (value == null) {
      return null;
    }
    checkReadOnlyModeSupported(value);
    V clone;
    if (readOnlyModeAdapter == null || !readOnlyModeAdapter.isApplicableTo(value)) {
      clone = cloneValue(value);
    } else {
      clone = value; // for read only objects we do not clone the returned object. -> skip call of: cloneValue(value);
    }
    return clone;
  }

  @Override
  public V cloneTxView2ReadOnlyTxView(V value) {
    if (value == null) {
      return null;
    }
    if (readOnlyModeAdapter != null && readOnlyModeAdapter.isApplicableTo(value)) {
      if (readOnlyModeAdapter.isReadOnly(value)) {
        return value;
      } else {
        return readOnlyModeAdapter.switchToReadOnlyMode(cloneValue(value));
      }
    } else {
      return cloneValue(value);
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

  private void checkReadOnlyModeSupported(V value) throws ReadOnlyModeNotSupportedException {
    if (throwIfMissingReadOnlyModeDetected && value != null && (readOnlyModeAdapter == null || !readOnlyModeAdapter.isApplicableTo(value))) {
      throw new ReadOnlyModeNotSupportedException("Object of class " + value.getClass().getName() + " not supporting read only mode! Object: " + value);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(readOnlyModeAdapter=" + readOnlyModeAdapter + ", throwIfMissingReadOnlyModeDetected=" + throwIfMissingReadOnlyModeDetected +")";
  }

}
