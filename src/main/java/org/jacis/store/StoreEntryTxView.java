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

  private final StoreEntry<K, TV, CV> committedEntry; // link to the committed entry (note this is the real committed instance that might be changed by other TXs)
  private TV txValue = null; // current value of the entry in this TX
  private TV origValue; // original value of the entry when cloning it to the transaction view (only tracked if configured)
  private long origVersion; // original version of the entry when cloning it to the transaction view (for optimistic locking)
  private boolean updated = false; // entry was updated in the current transaction

  StoreEntryTxView(StoreEntry<K, TV, CV> committedEntry, boolean trackOriginal) {
    JacisObjectAdapter<TV, CV> ca = committedEntry.getStore().getObjectAdapter();
    this.committedEntry = committedEntry;
    this.txValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
    this.origVersion = committedEntry.getVersion();
    if (trackOriginal) {
      origValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
    } else {
      origValue = null;
    }
  }

  StoreEntryTxView(StoreEntryTxView<K, TV, CV> orig) { // only to create a read only view
    this.committedEntry = orig.getCommittedEntry();
    JacisObjectAdapter<TV, CV> ca = committedEntry.getStore().getObjectAdapter();
    this.txValue = ca.cloneTxView2ReadOnlyTxView(orig.txValue);
    this.origValue = ca.cloneTxView2ReadOnlyTxView(orig.origValue);
    this.origVersion = orig.origVersion;
  }

  void updateValue(TV newValue) {
    this.txValue = newValue;
    this.updated = true;
  }

  StoreEntry<K, TV, CV> getCommittedEntry() {
    return committedEntry;
  }

  K getKey() {
    return committedEntry.getKey();
  }

  TV getValue() {
    return txValue;
  }

  boolean isNull() {
    return txValue == null;
  }

  boolean isNotNull() {
    return txValue != null;
  }

  TV getOrigValue() {
    return origValue;
  }

  long getOrigVersion() {
    return origVersion;
  }

  boolean isUpdated() {
    return updated;
  }

  boolean isStale(JacisStoreTxView<K, TV, CV> txView) {
    return origVersion < committedEntry.getVersion() || committedEntry.isLockedForOtherThan(txView);
  }

  void assertNotStale(JacisStoreTxView<K, TV, CV> txView) throws JacisStaleObjectException {
    if (committedEntry.isLockedForOtherThan(txView)) {
      throwStale(txView);
    } else if (origVersion < committedEntry.getVersion()) {
      throwStale(txView);
    }
  }

  private void throwStale(JacisStoreTxView<K, TV, CV> txView) throws JacisStaleObjectException {
    StringBuilder msg = new StringBuilder();
    msg.append("Object ").append(getKey());
    msg.append(" updated by current TX ").append(txView.getTxId()).append(" (from v. ").append(getOrigVersion()).append(")");
    JacisStoreTxView<K, TV, CV> otherTxView = committedEntry.getLockedFor() != null ? committedEntry.getLockedFor() : committedEntry.getUpdatedBy();
    if (committedEntry.isLockedForOtherThan(txView)) {
      msg.append(" was already updated by prepared other TX ");
    } else {
      msg.append(" was already updated by other TX ");
    }
    msg.append(otherTxView == null ? "?" : otherTxView.getTxId()).append("!");
    StringBuilder details = new StringBuilder();
    details.append("// Details: \n");
    details.append(" - value changed by this TX: ").append(getValue()).append("\n");
    if (committedEntry.getStore().getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      details.append(" - original value          : ").append(getOrigValue()).append(" (v. ").append(getOrigVersion()).append(")").append("\n");
    }
    details.append(" - committed value         : ").append(committedEntry.getValue()).append(" (v. ").append(committedEntry.getVersion()).append(")").append("\n");
    details.append(" - current TX: ").append(txView).append("\n");
    details.append(" - other TX: ").append(otherTxView).append("\n");
    details.append(" - store: ").append(this);
    throw new JacisStaleObjectException(msg.toString()).setDetails(details.toString());
  }

  @Override
  public int hashCode() {
    return committedEntry.hashCode();
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
    return committedEntry == null ? that.committedEntry == null : committedEntry.equals(that.committedEntry);
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
