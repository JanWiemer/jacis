/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * Representing the view of a store entry the current transaction currently sees.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
class StoreEntryTxView<K, TV, CV> {

  private final StoreEntry<K, TV, CV> commitedEntry; // link to the committed entry (note this is the real committed instance that might be changed by other TXs)
  private TV txValue = null; // current value of the entry in this TX
  private TV origValue; // original value of the entry when cloning it to the transaction view (only tracked if configured)
  private long origVersion; // original version of the entry when cloning it to the transaction view (for optimistic locking)
  private boolean updated = false; // entry was updated in the current transaction

  public StoreEntryTxView(StoreEntry<K, TV, CV> commitedEntry, boolean trackOriginal) {
    JacisObjectAdapter<TV, CV> ca = commitedEntry.getStore().getObjectAdapter();
    this.commitedEntry = commitedEntry;
    this.txValue = ca.cloneCommitted2WritableTxView(commitedEntry.getValue());
    this.origVersion = commitedEntry.getVersion();
    if (trackOriginal) {
      origValue = ca.cloneCommitted2WritableTxView(commitedEntry.getValue());
    } else {
      origValue = null;
    }
  }

  StoreEntryTxView(StoreEntryTxView<K, TV, CV> orig) { // only to create a read only view
    this.commitedEntry = orig.getCommitedEntry();
    JacisObjectAdapter<TV, CV> ca = commitedEntry.getStore().getObjectAdapter();
    this.txValue = ca.cloneTxView2ReadOnlyTxView(orig.txValue);
    this.origValue = ca.cloneTxView2ReadOnlyTxView(orig.origValue);
    this.origVersion = orig.origVersion;
  }

  public void updateValue(TV newValue) {
    this.txValue = newValue;
    this.updated = true;
  }

  public StoreEntry<K, TV, CV> getCommitedEntry() {
    return commitedEntry;
  }

  public K getKey() {
    return commitedEntry.getKey();
  }

  public TV getValue() {
    return txValue;
  }

  public boolean isNull() {
    return txValue == null;
  }

  public boolean isNotNull() {
    return txValue != null;
  }

  public TV getOrigValue() {
    return origValue;
  }

  public long getOrigVersion() {
    return origVersion;
  }

  public boolean isUpdated() {
    return updated;
  }

  public boolean isStale(JacisStoreTxView<K, TV, CV> txView) {
    if (origVersion < commitedEntry.getVersion()) {
      return true;
    }
    return commitedEntry.isLockedForOtherThan(txView);
  }

  public void assertNotStale(JacisStoreTxView<K, TV, CV> txView) {
    if (commitedEntry.isLockedForOtherThan(txView)) {
      throwStale(txView);
    } else if (origVersion < commitedEntry.getVersion()) {
      throwStale(txView);
    }
  }

  private void throwStale(JacisStoreTxView<K, TV, CV> txView) {
    StringBuilder msg = new StringBuilder();
    msg.append("Object ").append(getKey());
    msg.append(" updated by current TX ").append(txView.getTxId()).append(" (from v. ").append(getOrigVersion()).append(")");
    JacisStoreTxView<K, TV, CV> otherTxView = commitedEntry.getLockedFor() != null ? commitedEntry.getLockedFor() : commitedEntry.getUpdatedBy();
    if (commitedEntry.isLockedForOtherThan(txView)) {
      msg.append(" was already updated by prepared other TX ");
    } else {
      msg.append(" was already updated by other TX ");
    }
    msg.append(otherTxView == null ? "?" : otherTxView.getTxId()).append("!");
    StringBuilder details = new StringBuilder();
    details.append("// Details: \n");
    details.append(" - value changed by this TX: ").append(getValue()).append("\n");
    if (commitedEntry.getStore().getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      details.append(" - original value          : ").append(getOrigValue()).append(" (v. ").append(getOrigVersion()).append(")").append("\n");
    }
    details.append(" - committed value         : ").append(commitedEntry.getValue()).append(" (v. ").append(commitedEntry.getVersion()).append(")").append("\n");
    details.append(" - current TX: ").append(txView).append("\n");
    details.append(" - other TX: ").append(otherTxView).append("\n");
    details.append(" - store: ").append(this);
    throw new JacisStaleObjectException(msg.toString()).setDetails(details.toString());
  }

  @Override
  public int hashCode() {
    return commitedEntry.hashCode();
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
    StoreEntryTxView<?, ?, ?> that = (StoreEntryTxView<?, ?, ?>) obj;
    return commitedEntry == null ? that.commitedEntry == null : commitedEntry.equals(that.commitedEntry);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(getKey()).append("->").append(txValue);
    if (updated) {
      b.append(" (updated from v.").append(origVersion).append(":").append(origValue).append(")");
    } else {
      b.append(" (v.").append(origVersion).append(")");
    }
    return b.toString();
  }

}
