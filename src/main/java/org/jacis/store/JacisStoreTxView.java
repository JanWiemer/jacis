/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.index.JacisIndexRegistryTxView;
import org.jacis.trackedviews.TrackedView;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Representing the transactional view on the store entries for one transaction.
 * An entry is cloned to this view if a transaction reads it from the store unless it reads it in read only mode.
 * Modifications are always done only on the entries in the transactional view until the transaction is committed.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
class JacisStoreTxView<K, TV, CV> implements JacisReadOnlyTransactionContext {

  /**
   * the handle for the transaction this view belongs to
   */
  private final JacisTransactionHandle tx;
  /**
   * the creation timestamp in system milliseconds (timestamp usually set at first access returning a TX view)
   */
  private final long creationTimestamp;
  /**
   * the entries with an own view in this TX
   */
  private final Map<K, StoreEntryTxView<K, TV, CV>> storeTxView;
  /**
   * map of optimistic locks created for entries of the store
   */
  private Map<K, Long> optimisticLockVersionMap;
  /**
   * the name of the TX if this is a read only snapshot (null <-> writable)
   */
  private final String readOnlyTxId;
  /**
   * reference to the main store
   */
  private final JacisStoreImpl<K, TV, CV> store;
  /**
   * flag indicating if for the transaction a commit is pending, that means a prepare has already been called
   */
  private boolean commitPending = false;
  /**
   * flag indicating if the transaction is already committed
   */
  private boolean committed = false;
  /**
   * flag indicating if the transaction is rolled back
   */
  private boolean rolledBack = false;
  /**
   * gives the reason (null means valid) why the tx has been invalidated. Attempts to internalCommit the tx will be ignored.
   */
  private String invalidationReason = null;
  /**
   * the number of entries of this TX view
   */
  private int numberOfEntries = 0;
  /**
   * updated sequence
   */
  private int updateSequence = 0;
  /**
   * the number of updated entries of this TX view
   */
  private int numberOfUpdatedEntries = 0;
  /**
   * tracked views by this transaction view. The tracked views in this map are kept up-to-date during the current TX
   */
  private final Map<String, TrackedViewTransactionLocal<K, TV>> trackedViews;
  /**
   * transaction view of the index values
   */
  private final JacisIndexRegistryTxView<K, TV> indexRegistryTxView;
  /**
   * cached list of updated entries that have to be committed
   */
  private List<StoreEntryTxView<K, TV, CV>> updatedEntries;

  JacisStoreTxView(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction) {
    this.store = store;
    this.tx = transaction;
    this.readOnlyTxId = null;
    this.creationTimestamp = System.currentTimeMillis();
    this.storeTxView = new HashMap<>();
    this.trackedViews = new HashMap<>();
    this.indexRegistryTxView = new JacisIndexRegistryTxView<>(store.getIndexRegistry());
  }

  JacisStoreTxView(String readOnlyTxId, JacisStoreTxView<K, TV, CV> orig, boolean threadsafe) { // only to create a read only snapshot
    this.store = orig.store;
    this.tx = orig.tx;
    this.readOnlyTxId = readOnlyTxId;
    this.creationTimestamp = orig.creationTimestamp;
    Map<K, StoreEntryTxView<K, TV, CV>> origCache = orig.storeTxView;
    Map<K, StoreEntryTxView<K, TV, CV>> readOnlyCache = threadsafe ? new ConcurrentHashMap<>(origCache.size()) : new HashMap<>(origCache.size());
    for (Entry<K, StoreEntryTxView<K, TV, CV>> mapEntry : origCache.entrySet()) {
      StoreEntryTxView<K, TV, CV> cacheEntry = new StoreEntryTxView<>(mapEntry.getValue());
      readOnlyCache.put(mapEntry.getKey(), cacheEntry);
    }
    storeTxView = readOnlyCache;
    trackedViews = threadsafe ? new ConcurrentHashMap<>(orig.trackedViews) : new HashMap<>(orig.trackedViews); // Enable multithreaded access to read only context. See issue #30
    this.indexRegistryTxView = orig.indexRegistryTxView;
    numberOfEntries = storeTxView.size();
  }

  @Override
  public String getTxId() {
    return readOnlyTxId == null ? tx.getTxId() : readOnlyTxId + "|" + tx.getTxId();
  }

  @Override
  public String getReadOnlyTxId() {
    return readOnlyTxId;
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

  public boolean isCommitted() {
    return committed;
  }

  public boolean isRolledBack() {
    return rolledBack;
  }

  public int getNumberOfEntries() {
    return numberOfEntries;
  }

  public int getNumberOfUpdatedEntries() {
    return numberOfUpdatedEntries;
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

  Collection<StoreEntryTxView<K, TV, CV>> getUpdatedEntriesForCommit() {
    if (updatedEntries == null) {
      updatedEntries = new ArrayList<>();
      for (StoreEntryTxView<K, TV, CV> entry : getAllEntryTxViews()) {
        if (entry.isUpdated()) {
          updatedEntries.add(entry);
        }
      }
      if (store.getObjectTypeSpec().isTrackModificationInOrderOfUpdateCalls()) {
        Collections.sort(updatedEntries);
      }
    }
    return updatedEntries;
  }

  StoreEntryTxView<K, TV, CV> createTxViewEntry(StoreEntry<K, TV, CV> committedEntry) {
    StoreEntryTxView<K, TV, CV> entry = new StoreEntryTxView<>(committedEntry, store.getObjectTypeSpec().isTrackOriginalValueEnabled());
    storeTxView.put(entry.getKey(), entry);
    numberOfEntries = storeTxView.size();
    return entry;
  }

  boolean refreshTxViewEntryFromCommitted(K key, boolean forceIfUpdated) {
    StoreEntryTxView<K, TV, CV> entryTxView = storeTxView.get(key);
    if (entryTxView != null) {
      if (entryTxView.isUpdated()) {
        if (!forceIfUpdated) {
          return false;
        }
        numberOfUpdatedEntries--; // removed an updated element
      }
      boolean trackingRequired = isTrackingAtIndicesRequired();
      TV oldOrigValue = entryTxView.getOrigValue();
      if (trackingRequired) {
        TV prevValue = entryTxView.getLastUpdatedValue();
        if (prevValue == null) {
          prevValue = oldOrigValue;
        }
        entryTxView.refreshFromCommitted();
        entryTxView.trackLastUpdated();
        TV newValue = entryTxView.getValue();
        trackUpdateAtIndices(entryTxView.getKey(), prevValue, newValue);
      } else {
        entryTxView.refreshFromCommitted();
      }
      trackUpdateAtViews(oldOrigValue, entryTxView.getValue(), entryTxView);
    }
    return true;
  }

  void updateValue(StoreEntryTxView<K, TV, CV> entryTxView, TV newValue) {
    updateSequence++;
    if (!entryTxView.isUpdated()) {
      numberOfUpdatedEntries++; // a new updated element
    }
    boolean trackingRequired = isTrackingAtIndicesRequired();
    if (trackingRequired) {
      TV prevValue = entryTxView.getLastUpdatedValue();
      if (prevValue == null) {
        prevValue = entryTxView.getOrigValue();
      }
      entryTxView.updateValue(newValue, updateSequence);
      entryTxView.trackLastUpdated();
      trackUpdateAtIndices(entryTxView.getKey(), prevValue, newValue);
    } else {
      entryTxView.updateValue(newValue, updateSequence);
    }
    trackUpdateAtViews(entryTxView.getOrigValue(), newValue, entryTxView);
  }

  public void addOptimisticLock(K key, StoreEntry<K, TV, CV> committedEntry) {
    if (optimisticLockVersionMap == null) {
      optimisticLockVersionMap = new HashMap<>();
    }
    optimisticLockVersionMap.putIfAbsent(key, committedEntry.getVersion());
  }

  public void addOptimisticLock(K key, StoreEntryTxView<K, TV, CV> entryTxView) {
    if (optimisticLockVersionMap == null) {
      optimisticLockVersionMap = new HashMap<>();
    }
    optimisticLockVersionMap.putIfAbsent(key, entryTxView.getOrigVersion());
  }

  public void removeOptimisticLock(K key) {
    if (optimisticLockVersionMap != null) {
      optimisticLockVersionMap.remove(key);
    }
  }

  public Long getOptimisticLockVersion(K key) {
    return optimisticLockVersionMap == null ? null : optimisticLockVersionMap.get(key);
  }

  public Map<K, Long> getOptimisticLockVersionMap() {
    return optimisticLockVersionMap;
  }

  private boolean isTrackingAtIndicesRequired() {
    return indexRegistryTxView.isTrackingRequired();
  }

  private void trackUpdateAtIndices(K key, TV prevValue, TV newValue) {
    indexRegistryTxView.onTxLocalUpdate(key, prevValue, newValue);
  }

  private void trackUpdateAtViews(TV prevValue, TV newValue, StoreEntryTxView<K, TV, CV> entryTxView) {
    for (TrackedViewTransactionLocal<K, TV> trackedView : trackedViews.values()) {
      trackedView.trackModification(prevValue, newValue, entryTxView);
    }
  }

  void startCommitPhase() {
    this.commitPending = true;
  }

  void afterCommit() {
    committed = true;
    commitPending = false;
  }

  void afterRollback() {
    rolledBack = true;
    commitPending = false;
  }

  void destroy() {
    storeTxView.clear();
    if (!isReadOnly()) {
      store.notifyTxViewDestroyed(this);
    }
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

  @SuppressWarnings("unchecked")
  private <VT extends TrackedView<TV>> VT internalGetTrackedView(String internalViewKey, Supplier<VT> initialViewSupplier) {
    if (!this.trackedViews.containsKey(internalViewKey)) {
      VT view = initialViewSupplier.get();
      TrackedViewTransactionLocal<K, TV> local = new TrackedViewTransactionLocal<>(view);

      for (StoreEntryTxView<K, TV, ?> entryTxView : getAllEntryTxViews()) {
        local.trackModification(entryTxView.getOrigValue(), entryTxView.getValue(), entryTxView);
      }
      this.trackedViews.put(internalViewKey, local);
    }
    return (VT) this.trackedViews.get(internalViewKey).getTrackedView();
  }

  <VT extends TrackedView<TV>> VT getTrackedView(String viewName, Supplier<VT> initialViewSupplier) {
    return internalGetTrackedView("V:" + viewName, initialViewSupplier);
  }

  <VT extends TrackedView<TV>> VT getTrackedSubView(String viewName, Object subViewKey, Supplier<VT> initialViewSupplier) {
    return internalGetTrackedView("SV:" + viewName + "-" + subViewKey, initialViewSupplier);
  }

  boolean containsTrackedView(String viewName) {
    return this.trackedViews.containsKey(viewName);
  }

  boolean containsTrackedSubView(String viewName, Object subViewKey) {
    return this.trackedViews.containsKey("SV:" + viewName + "-" + subViewKey);
  }

  public JacisIndexRegistryTxView<K, TV> getIndexRegistryTxView() {
    return indexRegistryTxView;
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
    if (committed) {
      b.append("[COMMITTED]");
    }
    if (rolledBack) {
      b.append("[ROLLED-BACK]");
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
