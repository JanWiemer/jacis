/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

/**
 * Representing a committed version of an entry in the store.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
class StoreEntry<K, TV, CV> {

  /** reference to the main store */
  private final JacisStoreAdminInterface<K, TV, CV> store;
  /** the key of this entry */
  private final K key;
  /** the current committed valued of this entry (visible to all transactions) (null if not existing / deleted) */
  private CV value = null;
  /** version counter will be increased when an updated view of the entry is committed (used for optimistic locking) */
  private long version = 0;
  /** id of the transaction that has committed the current version (for logging / debugging only) */
  private String updatedBy = null;
  /** name of the thread that has committed the current version (for logging / debugging only) */
  private String updatedByThread = null;
  /** transaction this object is locked for (in the time between prepare and internalCommit) */
  private JacisStoreTxView<K, TV, CV> lockedFor = null;
  /** name of the thread that this object is locked for (in the time between prepare and internalCommit) */
  private String lockedForThread = null;

  StoreEntry(JacisStoreAdminInterface<K, TV, CV> store, K key) {
    this.store = store;
    this.key = key;
  }

  StoreEntry(JacisStoreAdminInterface<K, TV, CV> store, K key, TV value) { // only for the initial value
    this.store = store;
    this.key = key;
    this.value = store.getObjectAdapter().cloneTxView2Committed(value);
  }

  @SuppressWarnings("ObjectEquality")
  synchronized public void update(StoreEntryTxView<K, TV, CV> entryTxView, JacisStoreTxView<K, TV, CV> byTx) {
    TV txVal = entryTxView.getValue();
    if (txVal == null) { // deleted
      value = null;
    } else if (txVal != value) { // intentionally checked if both instances are different (and not used equals!)
      value = store.getObjectAdapter().cloneTxView2Committed(txVal);
    }
    version++;
    updatedBy = byTx.getTxId();
    updatedByThread = Thread.currentThread().getName();
  }

  synchronized void lockedFor(JacisStoreTxView<K, TV, CV> lockingTx) {
    lockedFor = lockingTx;
    lockedForThread = Thread.currentThread().getName();
  }

  synchronized void releaseLockedFor(JacisStoreTxView<K, TV, CV> releasingTx) {
    if (releasingTx.equals(getLockedFor())) {
      lockedFor = null;
      lockedForThread = null;
    }
  }

  synchronized boolean isLocked() {
    return lockedFor != null;
  }

  synchronized boolean isLockedForOtherThan(JacisStoreTxView<K, TV, CV> txView) {
    JacisStoreTxView<K, TV, CV> lf = lockedFor;
    return lf != null && !lf.equals(txView);
  }

  JacisStoreAdminInterface<K, TV, CV> getStore() {
    return store;
  }

  K getKey() {
    return key;
  }

  synchronized CV getValue() {
    return value;
  }

  synchronized boolean isNull() {
    return value == null;
  }

  synchronized boolean isNotNull() {
    return value != null;
  }

  synchronized long getVersion() {
    return version;
  }

  synchronized String getUpdatedByTxId() {
    return updatedBy;
  }

  synchronized String getUpdatedByThread() {
    return updatedByThread;
  }

  synchronized JacisStoreTxView<K, TV, CV> getLockedFor() {
    return lockedFor;
  }

  public String getLockedForThread() {
    return lockedForThread;
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
  synchronized public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(key).append("->").append(value).append(" (v.").append(version).append(")");
    JacisStoreTxView<K, TV, CV> lf = lockedFor;
    if (lf != null) {
      b.append("lockedFor:").append(lf);
    }
    return b.toString();
  }

}
