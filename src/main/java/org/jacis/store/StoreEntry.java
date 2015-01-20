package org.jacis.store;

/**
 * @author Jan Wiemer
 * 
 * Representing a committed version of an entry in the store.
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
class StoreEntry<K, V> {

  private final JacisStore<K, V> store;
  private final K key;
  private V value = null;
  private long version = 0; // version counter will be increased when an updated view of the entry is committed (used for optimistic locking)
  private String updatedBy = null; // transaction that has committed the current version (for logging / debugging only) 
  private JacisStoreTxView<K, V> lockedFor = null; // transaction this object is locked for (in the time between prepare and commit)

  public StoreEntry(JacisStore<K, V> store, K key) {
    this.store = store;
    this.key = key;
  }

  public void update(StoreEntryTxView<K, V> entryTxView, String byTxName) {
    V txVal = entryTxView.getValue();
    if (txVal == null) { // deleted
      value = null;
    } else if (txVal != value) { // intentionally checked if both instances are different (and not used equals!)
      value = store.getCloneHelper().cloneTxView2Committed(txVal);
    }
    version++;
    updatedBy = byTxName;
  }

  public void lockedFor(JacisStoreTxView<K, V> lockingTx) {
    lockedFor = lockingTx;
  }

  public void releaseLockedFor(JacisStoreTxView<K, V> releasingTx) {
    if (releasingTx.equals(getLockedFor())) {
      lockedFor = null;
    }
  }

  public boolean isLocked() {
    return lockedFor != null;
  }

  public boolean isLockedForOtherThan(JacisStoreTxView<K, V> txView) {
    return lockedFor != null && !lockedFor.equals(txView);
  }

  public JacisStore<K, V> getStore() {
    return store;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
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

  public String getUpdatedBy() {
    return updatedBy;
  }

  public JacisStoreTxView<K, V> getLockedFor() {
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
    StoreEntry<?, ?> that = (StoreEntry<?, ?>) obj;
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
