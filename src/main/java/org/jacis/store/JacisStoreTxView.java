/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisTransactionHandle;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Jan Wiemer
 * 
 * Representing a committed version of an entry in the store.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 */
class JacisStoreTxView<K, TV, CV> {

  private final JacisTransactionHandle tx; // the transaction
  private final long creationTimestamp; // in system milliseconds (timestamp usually set at first access returning a TX view)
  private final Map<K, StoreEntryTxView<K, TV, CV>> storeTxView; // entries with an own view in this TX
  private final String readOnlyTxName; // the name of the TX if this is a read only snapshot (null <-> writable)
  private JacisStore<K, TV, CV> store; // main store
  private boolean commitPending = false; // commit pending / prepare already called
  private String invalidationReason = null; // gives the reason (null means valid) why the tx has been invalidated. Attempts to commit the tx will be ignored.

  public JacisStoreTxView(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    this.store = store;
    this.tx = transaction;
    this.readOnlyTxName = null;
    this.creationTimestamp = System.currentTimeMillis();
    this.storeTxView = new HashMap<>();
  }

  public JacisStoreTxView(String readOnlyTxName, JacisStoreTxView<K, TV, CV> orig) { // only to create a read only snapshot
    this.tx = orig.tx;
    this.readOnlyTxName = readOnlyTxName;
    this.creationTimestamp = orig.creationTimestamp;
    Map<K, StoreEntryTxView<K, TV, CV>> origCache = orig.storeTxView;
    Map<K, StoreEntryTxView<K, TV, CV>> readOnlyCache = new HashMap<>(origCache.size());
    for (Entry<K, StoreEntryTxView<K, TV, CV>> mapEntry : origCache.entrySet()) {
      StoreEntryTxView<K, TV, CV> cacheEntry = new StoreEntryTxView<>(mapEntry.getValue());
      readOnlyCache.put(mapEntry.getKey(), cacheEntry);
    }
    storeTxView = readOnlyCache;
  }

  public String getTxName() {
    return readOnlyTxName == null ? tx.getTxName() : readOnlyTxName + "|" + tx.getTxName();
  }

  public String getTxShortName() {
    return tx.getTxShortName();
  }

  public JacisTransactionHandle getTransaction() {
    return tx;
  }

  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  public boolean isReadOnly() {
    return readOnlyTxName != null;
  }

  public boolean isCommitPending() {
    return commitPending;
  }

  public JacisStoreTxView<K, TV, CV> assertWritable() {
    if (commitPending) {
      throw new IllegalStateException("Commit already started for transaction " + this);
    } else if (isReadOnly()) {
      throw new IllegalStateException("No changes allowed for read only transaction " + this);
    }
    return this;
  }

  public boolean containsTxView(K key) {
    return storeTxView.containsKey(key);
  }

  public StoreEntryTxView<K, TV, CV> getEntryTxView(K key) {
    return storeTxView.get(key);
  }

  public Collection<StoreEntryTxView<K, TV, CV>> getAllEntryTxViews() {
    return storeTxView.values();
  }

  public StoreEntryTxView<K, TV, CV> createTxViewEntry(StoreEntry<K, TV, CV> commitedEntry) {
    StoreEntryTxView<K, TV, CV> entry = new StoreEntryTxView<>(commitedEntry, store.getObjectTypeSpec().isTrackOriginalValueEnabled());
    storeTxView.put(entry.getKey(), entry);
    return entry;
  }

  public boolean removeTxViewEntry(K key, boolean forceIfUpdated) {
    StoreEntryTxView<K, TV, CV> entry = storeTxView.get(key);
    if (entry.isUpdated() && !forceIfUpdated) {
      return false;
    }
    storeTxView.remove(key);
    return true;
  }

  public void startCommitPhase() {
    this.commitPending = true;
  }

  public void destroy() {
    storeTxView.clear();
    store.notifyTxViewDestroyed(this);
  }

  public boolean isInvalidated() {
    return invalidationReason != null;
  }

  public String getInvalidationReason() {
    return invalidationReason;
  }

  public void invalidate(String reason) {
    this.invalidationReason = reason;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (readOnlyTxName != null) {
      b.append(readOnlyTxName).append("(snapshot-from:").append(tx).append(")");
    } else {
      b.append(tx);
    }
    if (commitPending) {
      b.append("[COMMIT-PENDING]");
    }
    b.append("(#entries=").append(storeTxView.size()).append(")");
    if (isReadOnly()) {
      b.append("[readOnly]");
    }
    if (isInvalidated()) {
      b.append("[INVALIDATED because ").append(getInvalidationReason()).append("]");
    }
    return b.toString();
  }

}
