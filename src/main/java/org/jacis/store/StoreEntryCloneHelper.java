package org.jacis.store;

import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.plugin.cloning.JacisStoreEntryCloneAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

public class StoreEntryCloneHelper<V> {

  private final JacisStoreEntryCloneAdapter<V> cloneAdapter;
  private final JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters;

  public StoreEntryCloneHelper(JacisObjectTypeSpec<?, V> objectSpec) {
    this.cloneAdapter = objectSpec.getCloneAdapter();
    this.readOnlyModeAdapters = objectSpec.getReadOnlyModeAdapter();
  }

  public V cloneCommitted2WritableTxView(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneAdapter.cloneValue(value);
    return readOnlyModeAdapters.switchToReadWriteMode(clone);
  }

  public V cloneCommitted2ReadOnlyTxView(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneAdapter.cloneValue(value);
    return clone;
  }

  public V cloneTxView2Committed(V value) {
    if (value == null) {
      return null;
    }
    V clone = cloneAdapter.cloneValue(value);
    return readOnlyModeAdapters.switchToReadOnlyMode(clone);
  }

}
