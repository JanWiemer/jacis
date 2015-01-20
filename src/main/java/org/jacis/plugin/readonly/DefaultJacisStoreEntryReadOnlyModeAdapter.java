package org.jacis.plugin.readonly;

public class DefaultJacisStoreEntryReadOnlyModeAdapter<V> implements JacisStoreEntryReadOnlyModeAdapter<V> {

  public DefaultJacisStoreEntryReadOnlyModeAdapter() {
    // TODO Auto-generated constructor stub
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
