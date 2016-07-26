/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

/**
 * @author Jan Wiemer
 * 
 * Representing a committed version of an entry in the store.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 */
class StoreEntry<K, TV, CV> {

  private final JacisStore<K, TV, CV> store;
  private final K key;
  private CV value = null;
  private long version = 0; // version counter will be increased when an updated view of the entry is committed (used for optimistic locking)
  private JacisStoreTxView<K, TV, CV> updatedBy = null; // transaction that has committed the current version (for logging / debugging only) 
  private JacisStoreTxView<K, TV, CV> lockedFor = null; // transaction this object is locked for (in the time between prepare and internalCommit)

  public StoreEntry(JacisStore<K, TV, CV> store, K key) {
    this.store = store;
    this.key = key;
  }

  public void update(StoreEntryTxView<K, TV, CV> entryTxView, JacisStoreTxView<K, TV, CV> byTx) {
    TV txVal = entryTxView.getValue();
    if (txVal == null) { // deleted
      value = null;
    } else if (txVal != value) { // intentionally checked if both instances are different (and not used equals!)
      value = store.getObjectAdapter().cloneTxView2Committed(txVal);
    }
    version++;
    updatedBy = byTx;
  }

  public void lockedFor(JacisStoreTxView<K, TV, CV> lockingTx) {
    lockedFor = lockingTx;
  }

  public void releaseLockedFor(JacisStoreTxView<K, TV, CV> releasingTx) {
    if (releasingTx.equals(getLockedFor())) {
      lockedFor = null;
    }
  }

  public boolean isLocked() {
    return lockedFor != null;
  }

  public boolean isLockedForOtherThan(JacisStoreTxView<K, TV, CV> txView) {
    return lockedFor != null && !lockedFor.equals(txView);
  }

  public JacisStore<K, TV, CV> getStore() {
    return store;
  }

  public K getKey() {
    return key;
  }

  public CV getValue() {
    return value;
  }

  public boolean isNull() {
    return value == null;
  }

  public boolean isNotNull() {
    return value != null;
  }

  public long getVersion() {
    return version;
  }

  public JacisStoreTxView<K, TV, CV> getUpdatedBy() {
    return updatedBy;
  }

  public JacisStoreTxView<K, TV, CV> getLockedFor() {
    return lockedFor;
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (getClass() != obj.getClass()) {
      return false;
    }
    StoreEntry<?, ?, ?> that = (StoreEntry<?, ?, ?>) obj;
    return key == null ? that.key == null : key.equals(that.key);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(key).append("->").append(value).append(" (v.").append(version).append(")");
    if (lockedFor != null) {
      b.append("lockedFor:").append(lockedFor);
    }
    return b.toString();
  }

}
