/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.readonly;

public class DefaultJacisStoreEntryReadOnlyModeAdapter<V> implements JacisStoreEntryReadOnlyModeAdapter<V> {

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
  public boolean isReadOnly(V value) {
    return ((JacisReadonlyModeSupport) value).isReadOnly();
  }

}
