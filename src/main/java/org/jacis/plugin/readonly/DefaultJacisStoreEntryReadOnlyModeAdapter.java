/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.readonly;

import org.jacis.plugin.readonly.object.JacisReadonlyModeSupport;

/**
 * The default implementation of the interface {@link JacisStoreEntryReadOnlyModeAdapter}.
 * This implementation is applicable for objects
 * implementing the {@link org.jacis.plugin.readonly.object.JacisReadonlyModeSupport} interface
 * and uses the methods declared in this interface for switching the mode.
 *
 * @param <V> The type of the values that should be switched between read-write and read-only mode.
 * @author Jan Wiemer
 */
public class DefaultJacisStoreEntryReadOnlyModeAdapter<V> implements JacisStoreEntryReadOnlyModeAdapter<V> {

  @Override
  public boolean isApplicableTo(V value) {
    return value instanceof JacisReadonlyModeSupport;
  }

  @Override
  public boolean isReadOnly(V value) {
    return ((JacisReadonlyModeSupport) value).isReadOnly();
  }

  @Override
  public V switchToReadOnlyMode(V value) {
    ((JacisReadonlyModeSupport) value).switchToReadOnlyMode();
    return value;
  }

  @Override
  public V switchToReadWriteMode(V value) {
    ((JacisReadonlyModeSupport) value).switchToReadWriteMode();
    return value;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
