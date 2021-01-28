/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.JacisApi;
import org.jacis.exception.ReadOnlyModeNotSupportedException;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * Abstract generic implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter}
 * copying the objects to and from the transactional view by cloning the object.
 * The method used to clone the object is left to the derived class implementing the {@link #cloneValue(Object)} method.
 * 
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
@JacisApi
public abstract class AbstractJacisCloningObjectAdapter<V> implements JacisObjectAdapter<V, V> {

  /** Read only mode adapter used to switch objects from writable to read only mode if required and supported */
  private final JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter;
  /** Flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported. */
  private boolean throwIfMissingReadOnlyModeDetected = false;

  /**
   * Create a cloning object adapter with the passed read only mode adapter.
   * 
   * @param readOnlyModeAdapters Adapter to switch an object between read-only and read-write mode (if supported).
   */
  @SuppressWarnings("WeakerAccess")
  public AbstractJacisCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    this.readOnlyModeAdapter = readOnlyModeAdapters;
  }

  /**
   * Create a cloning object adapter with a default read only mode adapter (see {@link DefaultJacisStoreEntryReadOnlyModeAdapter}).
   */
  public AbstractJacisCloningObjectAdapter() {
    this(new DefaultJacisStoreEntryReadOnlyModeAdapter<>());
  }

  /**
   * Set the flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported.
   * 
   * @param throwIfMissingReadOnlyModeDetected flag indicating if the object adapter should throw an exception if a read only mode is required, but not supported.
   * @return The current instance for method chaining
   */
  public AbstractJacisCloningObjectAdapter<V> setThrowIfMissingReadOnlyModeDetected(boolean throwIfMissingReadOnlyModeDetected) {
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

  private void checkReadOnlyModeSupported(V value) throws ReadOnlyModeNotSupportedException {
    if (throwIfMissingReadOnlyModeDetected && value != null && (readOnlyModeAdapter == null || !readOnlyModeAdapter.isApplicableTo(value))) {
      throw new ReadOnlyModeNotSupportedException("Object of class " + value.getClass().getName() + " not supporting read only mode! Object: " + value);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(readOnlyModeAdapter=" + readOnlyModeAdapter + ", throwIfMissingReadOnlyModeDetected=" + throwIfMissingReadOnlyModeDetected + ")";
  }

  protected abstract V cloneValue(V value);

}
