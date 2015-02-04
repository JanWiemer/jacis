package org.jacis.store;

/**
 * @author Jan Wiemer
 * 
 * State information regarding a store entry
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public class StoreEntryInfo<K, TV, CV> {

  private final K key;
  private final long committedVersion;
  private final String committedVersionLastCommitter;
  private final String committedVersionLockedForTx;
  private final long txViewOrigVersion;
  private final boolean txViewUpdated;
  private final boolean txViewStale;
  private final String committedValueString;
  private final String txViewValueString;
  private final String originalTxViewValueString;

  public StoreEntryInfo(K key, StoreEntry<K, TV, CV> committedEntry, StoreEntryTxView<K, TV, CV> entryTxView, JacisStoreTxView<K, TV, CV> txView) {
    this.key = key;
    if (committedEntry != null) {
      committedVersion = committedEntry.getVersion();
      committedVersionLastCommitter = committedEntry.getUpdatedBy();
      committedValueString = String.valueOf(committedEntry.getValue());
      JacisStoreTxView<K, TV, CV> lf = committedEntry.getLockedFor();
      committedVersionLockedForTx = lf == null ? null : lf.getTxName();
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
    b.append(", comittedVal=").append(committedValueString);
    b.append(" (v. ").append(committedVersion).append(committedVersionLockedForTx != null ? "-locked" : "").append(")");
    return b.toString();
  }
}
