/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning.readonly;

/**
 * @param <V>
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


}
