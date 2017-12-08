/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

/**
 * State information regarding a store entry
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class StoreEntryInfo<K, TV> {

  /** the key of the entry */
  private final K key;
  /** version of the committed entry in the core store */
  private final long committedVersion;
  /** transaction ID of the transaction that committed the committed entry */
  private final String committedVersionLastCommitter;
  /** transaction ID of the transaction currently locking the committed entry (for commit) */
  private final String committedVersionLockedForTx;
  /** original version of the transactional view of the entry (the version from the committet entry at the time it was cloned to the TX view) */
  private final long txViewOrigVersion;
  /** flag indicating if the transaction view has been updated */
  private final boolean txViewUpdated;
  /** flag indicating if the transaction view is stale (the committed version has been changed meanwhile) */
  private final boolean txViewStale;
  /** String representation of the committed value of the entry */
  private final String committedValueString;
  /** String representation of the value of the transactional view of the entry */
  private final String txViewValueString;
  /** String representation of the original value of the transactional view of the entry (az the time it was cloned to the TX view) */
  private final String originalTxViewValueString;

  <CV> StoreEntryInfo(K key, StoreEntry<K, TV, CV> committedEntry, StoreEntryTxView<K, TV, CV> entryTxView, JacisStoreTxView<K, TV, CV> txView) {
    this.key = key;
    if (committedEntry != null) {
      committedVersion = committedEntry.getVersion();
      committedVersionLastCommitter = committedEntry.getUpdatedByTxId();
      committedValueString = String.valueOf(committedEntry.getValue());
      JacisStoreTxView<K, TV, ?> lf = committedEntry.getLockedFor();
      committedVersionLockedForTx = lf == null ? null : lf.getTxId();
    } else {
      committedVersion = -1;
      committedVersionLastCommitter = null;
      committedValueString = null;
      committedVersionLockedForTx = null;
    }
    if (entryTxView != null) {
      txViewValueString = String.valueOf(entryTxView.getValue());
      originalTxViewValueString = String.valueOf(entryTxView.getOrigValue());
      txViewOrigVersion = entryTxView.getOrigVersion();
      txViewUpdated = entryTxView.isUpdated();
      txViewStale = entryTxView.isStale(txView);
    } else {
      txViewValueString = null;
      originalTxViewValueString = null;
      txViewOrigVersion = committedVersion;
      txViewUpdated = false;
      txViewStale = false;
    }
  }

  public K getKey() {
    return key;
  }

  public boolean exists() {
    return committedVersion >= 0;
  }

  public long getCommittedVersion() {
    return committedVersion;
  }

  public String getCommittedVersionLastCommitter() {
    return committedVersionLastCommitter;
  }

  public String getCommittedVersionLockedForTx() {
    return committedVersionLockedForTx;
  }

  public long getTxViewOrigVersion() {
    return txViewOrigVersion;
  }

  public boolean hasTxView() {
    return txViewValueString != null;
  }

  public boolean isTxViewUpdated() {
    return txViewUpdated;
  }

  public boolean isTxViewStale() {
    return txViewStale;
  }

  public String getCommittedValueString() {
    return committedValueString;
  }

  public String getTxViewValueString() {
    return txViewValueString;
  }

  public String getOriginalTxViewValueString() {
    return originalTxViewValueString;
  }

  public String toShortString() {
    StringBuilder b = new StringBuilder();
    b.append("key=").append(key);
    if (hasTxView()) {
      b.append(" TxView(v. ").append(txViewOrigVersion).append(txViewUpdated ? "+" : "").append(")");
    } else {
      b.append(" Np-TxView");
    }
    if (txViewStale) {
      b.append("[STALE]");
    }
    b.append(" Committed(v. ").append(committedVersion).append(committedVersionLockedForTx != null ? "-locked" : "").append(")");
    return b.toString();
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("key=").append(key);
    b.append(", txVal=").append(txViewValueString == null ? "-" : txViewValueString);
    b.append(", txOrigVal=").append(originalTxViewValueString == null ? "-" : originalTxViewValueString);
    b.append(" (v. ").append(txViewOrigVersion).append(txViewUpdated ? "+" : "").append(")");
    if (txViewStale) {
      b.append("[STALE]");
    }
    b.append(", committedVal=").append(committedValueString);
    b.append(" (v. ").append(committedVersion).append(committedVersionLockedForTx != null ? "-locked" : "").append(")");
    return b.toString();
  }
}
