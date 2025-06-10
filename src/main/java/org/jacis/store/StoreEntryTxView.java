/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.plugin.readonly.object.JacisReadonlyModeSupport;

import java.util.Objects;

/**
 * Representing the view of a store entry the current transaction currently sees.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
class StoreEntryTxView<K, TV, CV> implements Comparable<StoreEntryTxView<K, TV, CV>> {

  /** link to the committed entry (note this is the real committed instance that might be changed by other TXs) */
  private final StoreEntry<K, TV, CV> committedEntry;
  /** current value of the entry in this TX */
  private TV txValue = null;
  /** original value of the entry when cloning it to the transaction view (only tracked if configured) */
  private TV origValue;
  /** value of the entry when the last update was called */
  private TV lastUpdatedValue;
  /** original version of the entry when cloning it to the transaction view (for optimistic locking) */
  private long origVersion;
  /** flag indicating if entry was updated in the current transaction (initially false) */
  private int updated = 0;

  StoreEntryTxView(StoreEntry<K, TV, CV> committedEntry, boolean trackOriginal) {
    this.committedEntry = committedEntry;
    this.committedEntry.referencedByTxView();
    JacisObjectAdapter<TV, CV> ca = committedEntry.getStore().getObjectAdapter();
    this.txValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
    this.origVersion = committedEntry.getVersion();
    if (trackOriginal) {
      origValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
      if (origValue instanceof JacisReadonlyModeSupport) {
        ((JacisReadonlyModeSupport) origValue).switchToReadOnlyMode();
      }
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

  StoreEntryTxView(StoreEntry<K, TV, CV> committedEntry, long lockedVersion) { // for optimistic locking without cloning (lockReadOnly)
    this.committedEntry = committedEntry;
    this.txValue = null;
    this.origVersion = lockedVersion;
    origValue = null;
  }

  @Override
  public int compareTo(StoreEntryTxView<K, TV, CV> that) {
    return Integer.compare(this.updated, that.updated); // try to preserve update order
  }

  void updateValue(TV newValue, int updateSeq) {
    this.txValue = newValue;
    this.updated = updateSeq;
  }

  void refreshFromCommitted() {
    JacisObjectAdapter<TV, CV> ca = committedEntry.getStore().getObjectAdapter();
    this.txValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
    this.origVersion = committedEntry.getVersion();
    if (origValue != null) {
      origValue = ca.cloneCommitted2WritableTxView(committedEntry.getValue());
    } else {
      origValue = null;
    }
    this.updated = 0;
  }

  void trackLastUpdated() {
    JacisObjectAdapter<TV, CV> ca = committedEntry.getStore().getObjectAdapter();
    CV clone = ca.cloneTxView2Committed(this.txValue);
    this.lastUpdatedValue = ca.cloneCommitted2ReadOnlyTxView(clone);
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

  public TV getLastUpdatedValue() {
    return lastUpdatedValue;
  }

  boolean isUpdated() {
    return updated > 0;
  }

  boolean isStale(JacisStoreTxView<K, TV, CV> txView) {
    StoreEntry<K, TV, CV> theCommittedEntry = committedEntry;
    return theCommittedEntry == null || origVersion < theCommittedEntry.getVersion() || theCommittedEntry.isLockedForOtherThan(txView);
  }

  void assertNotStale(JacisStoreTxView<K, TV, CV> txView) throws JacisStaleObjectException {
    StoreEntry<K, TV, CV> theCommittedEntry = committedEntry;
    if (theCommittedEntry.isLockedForOtherThan(txView)) {
      throwStale(theCommittedEntry, txView);
    } else if (origVersion < committedEntry.getVersion()) {
      throwStale(theCommittedEntry, txView);
    }
  }

  private void throwStale(StoreEntry<K, TV, CV> theCommittedEntry, JacisStoreTxView<K, TV, CV> txView) throws JacisStaleObjectException {
    JacisStoreAdminInterface<K, TV, CV> store = theCommittedEntry.getStore();
    StringBuilder msg = new StringBuilder();
    msg.append("Object ").append(getKey());
    msg.append(" updated by current TX ").append(txView.getTxId()).append(" (from v. ").append(getOrigVersion()).append(")");
    JacisStoreTxView<K, TV, CV> lockedFor = theCommittedEntry.getLockedFor();
    String otherTxId;
    String otherThreadId;
    if (lockedFor != null) {
      otherTxId = lockedFor.getTxId();
      otherThreadId = theCommittedEntry.getLockedForThread();
    } else {
      otherTxId = committedEntry.getUpdatedByTxId();
      otherThreadId = committedEntry.getUpdatedByThread();
    }
    if (lockedFor != null && !lockedFor.equals(txView)) {
      msg.append(" was already updated by prepared other TX ");
    } else {
      msg.append(" was already updated by other TX ");
    }
    msg.append(otherTxId == null ? "?" : otherTxId);
    msg.append(" (to v. ").append(theCommittedEntry.getVersion()).append(")");
    msg.append(" (Store: ").append(store.getStoreIdentifier().toShortString());
    msg.append(", Thread: ").append(otherThreadId).append(")!");
    StringBuilder details = new StringBuilder();
    details.append("// Details: \n");
    details.append(" - value changed by this TX: ").append(getValue()).append("\n");
    if (store.getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      details.append(" - original value          : ").append(getOrigValue()).append(" (v. ").append(getOrigVersion()).append(")").append("\n");
    }
    details.append(" - committed value         : ").append(theCommittedEntry.getValue()).append(" (v. ").append(theCommittedEntry.getVersion()).append(")").append("\n");
    details.append(" - current TX: ").append(txView).append("\n");
    if (lockedFor != null) {
      details.append(" - other TX: ").append(lockedFor).append(" (Thread: ").append(otherThreadId).append(") (preparing)\n");
    } else {
      details.append(" - other TX: ").append(theCommittedEntry.getUpdatedByTxId()).append(" (Thread: ").append(otherThreadId).append(")\n");
    }
    details.append(" - store: ").append(store);
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
    return Objects.equals(committedEntry, that.committedEntry);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(getKey()).append("->").append(txValue);
    if (isUpdated()) {
      b.append(" (updated from v.").append(origVersion).append(":").append(origValue).append(")");
    } else {
      b.append(" (v.").append(origVersion).append(")");
    }
    return b.toString();
  }

}
