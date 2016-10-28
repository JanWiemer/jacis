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
 * Representing the transactional view on the store entries for one transaction.
 * An entry is cloned to this view if a transaction reads it from the store unless it reads it in read only mode.
 * Modifications are always done only on the entries in the transactional view until the transaction is committed.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
class JacisStoreTxView<K, TV, CV> {

  /** the handle for the transaction this view belongs to*/
  private final JacisTransactionHandle tx;
  /** the creation timestamp in system milliseconds (timestamp usually set at first access returning a TX view)*/
  private final long creationTimestamp;
  /** the entries with an own view in this TX */
  private final Map<K, StoreEntryTxView<K, TV, CV>> storeTxView;
  /** the name of the TX if this is a read only snapshot (null <-> writable) */
  private final String readOnlyTxId;
  /** reference to the main store */
  private JacisStore<K, TV, CV> store;
  /** flag indicating if for the transaction a commit is pending, that means a prepare has already been called */
  private boolean commitPending = false;
  /** gives the reason (null means valid) why the tx has been invalidated. Attempts to internalCommit the tx will be ignored. */
  private String invalidationReason = null;

  JacisStoreTxView(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    this.store = store;
    this.tx = transaction;
    this.readOnlyTxId = null;
    this.creationTimestamp = System.currentTimeMillis();
    this.storeTxView = new HashMap<>();
  }

  JacisStoreTxView(String readOnlyTxId, JacisStoreTxView<K, TV, CV> orig) { // only to create a read only snapshot
    this.tx = orig.tx;
    this.readOnlyTxId = readOnlyTxId;
    this.creationTimestamp = orig.creationTimestamp;
    Map<K, StoreEntryTxView<K, TV, CV>> origCache = orig.storeTxView;
    Map<K, StoreEntryTxView<K, TV, CV>> readOnlyCache = new HashMap<>(origCache.size());
    for (Entry<K, StoreEntryTxView<K, TV, CV>> mapEntry : origCache.entrySet()) {
      StoreEntryTxView<K, TV, CV> cacheEntry = new StoreEntryTxView<>(mapEntry.getValue());
      readOnlyCache.put(mapEntry.getKey(), cacheEntry);
    }
    storeTxView = readOnlyCache;
  }

  String getTxId() {
    return readOnlyTxId == null ? tx.getTxId() : readOnlyTxId + "|" + tx.getTxId();
  }

  String getTxDescription() {
    return tx.getTxDescription();
  }

  JacisTransactionHandle getTransaction() {
    return tx;
  }

  long getCreationTimestamp() {
    return creationTimestamp;
  }

  boolean isReadOnly() {
    return readOnlyTxId != null;
  }

  boolean isCommitPending() {
    return commitPending;
  }

  JacisStoreTxView<K, TV, CV> assertWritable() {
    if (commitPending) {
      throw new IllegalStateException("Commit already started for transaction " + this);
    } else if (isReadOnly()) {
      throw new IllegalStateException("No changes allowed for read only transaction " + this);
    }
    return this;
  }

  boolean containsTxView(K key) {
    return storeTxView.containsKey(key);
  }

  StoreEntryTxView<K, TV, CV> getEntryTxView(K key) {
    return storeTxView.get(key);
  }

  Collection<StoreEntryTxView<K, TV, CV>> getAllEntryTxViews() {
    return storeTxView.values();
  }

  StoreEntryTxView<K, TV, CV> createTxViewEntry(StoreEntry<K, TV, CV> committedEntry) {
    StoreEntryTxView<K, TV, CV> entry = new StoreEntryTxView<>(committedEntry, store.getObjectTypeSpec().isTrackOriginalValueEnabled());
    storeTxView.put(entry.getKey(), entry);
    return entry;
  }

  boolean removeTxViewEntry(K key, boolean forceIfUpdated) {
    StoreEntryTxView<K, TV, CV> entry = storeTxView.get(key);
    if (entry.isUpdated() && !forceIfUpdated) {
      return false;
    }
    storeTxView.remove(key);
    return true;
  }

  void startCommitPhase() {
    this.commitPending = true;
  }

  void destroy() {
    storeTxView.clear();
    store.notifyTxViewDestroyed(this);
  }

  boolean isInvalidated() {
    return invalidationReason != null;
  }

  String getInvalidationReason() {
    return invalidationReason;
  }

  void invalidate(String reason) {
    this.invalidationReason = reason;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (readOnlyTxId != null) {
      b.append(readOnlyTxId).append("(snapshot-from:").append(tx).append(")");
    } else {
      b.append(tx);
    }
    b.append("(#entries=").append(storeTxView.size()).append(")");
    if (commitPending) {
      b.append("[COMMIT-PENDING]");
    }
    if (isReadOnly()) {
      b.append("[READ-ONLY]");
    }
    if (isInvalidated()) {
      b.append("[INVALIDATED because ").append(getInvalidationReason()).append("]");
    }
    return b.toString();
  }

}
