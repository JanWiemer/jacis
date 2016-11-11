/*
 * Copyright (c) 2016. Jan Wiemer
 */
package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.exception.JacisTransactionAlreadyPreparedForCommitException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Storing a single type of objects.
 *
 * All operations checking or returning entries of the store operate on the committed values merged with the
 * current transactional view (obtained with the currently active transaction handle from the map {@link #txViewMap}).
 * This means that first the transactional view is checked if it contains an entry for the desired key.
 * If so this entry is returned, otherwise the committed value from the core store (see {@link #store}) is returned.
 * Note that if an object is deleted in a transaction an entry with the value 'null' remains in the transactional view.
 * Therefore also deletions are properly handled with respect to isolation.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public class JacisStoreImpl<K, TV, CV> extends JacisContainer.JacisStoreTransactionAdapter implements JacisStore<K, TV>, JacisStoreAdminInterface<K, TV, CV> {

  /** Reference to the JACIS container this store belongs to */
  private final JacisContainer container;
  /** The store identifier uniquely identifying this store inside the container */
  private final StoreIdentifier storeIdentifier;
  /** The object type specification for the objects stored in this store*/
  private final JacisObjectTypeSpec<K, TV, CV> spec;
  /** The map containing the committed values of the objects (the core store) */
  private final ConcurrentHashMap<K, StoreEntry<K, TV, CV>> store = new ConcurrentHashMap<>();
  /** A Map assigning each active transaction handle the transactional view on this store */
  private final Map<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>> txViewMap = Collections.synchronizedMap(new WeakHashMap<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>>());
  /** Mutex / Lock to synchronize changes on the committed entries of the store (specially during internalCommit) */
  private final ReadWriteLock storeAccessLock = new ReentrantReadWriteLock(true);
  /** The object adapter defining how to copy objects from the committed view to a transactional view and back */
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  /** The registry of tracked views for this store that are kept up to date on each commit automatically */
  private final TrackedViewRegistry<K, TV> trackedViewRegistry;
  /** List of listeners notified on each modification on the committed values in the store */
  private final List<JacisModificationListener<K, TV>> modificationListeners = new ArrayList<>();

  public JacisStoreImpl(JacisContainer container, StoreIdentifier storeIdentifier, JacisObjectTypeSpec<K, TV, CV> spec) {
    this.container = container;
    this.storeIdentifier = storeIdentifier;
    this.spec = spec;
    this.objectAdapter = spec.getObjectAdapter();
    this.trackedViewRegistry = new TrackedViewRegistry<>(this, spec.isCheckViewsOnCommit());
    registerModificationListener(trackedViewRegistry);
  }

  @Override
  public JacisStore<K, TV> getStore() {
    return this;
  }

  @Override
  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  @Override
  public JacisContainer getContainer() {
    return container;
  }

  @Override
  public JacisObjectTypeSpec<K, TV, CV> getObjectTypeSpec() {
    return spec;
  }

  @Override
  public List<JacisModificationListener<K, TV>> getModificationListeners() {
    return modificationListeners;
  }

  @Override
  public JacisStore<K, TV> registerModificationListener(JacisModificationListener<K, TV> listener) {
    if (!getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering modification listeners is only supported if original values are tracked, but they are not tracked for " + this + "! Trying to register listener: " + listener);
    }
    modificationListeners.add(listener);
    return this;
  }

  @Override
  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  @Override
  public TrackedViewRegistry<K, TV> getTrackedViewRegistry() {
    return trackedViewRegistry;
  }

  @Override
  public JacisReadOnlyTransactionContext createReadOnlyTransactionView(String withTxName) {
    JacisStoreTxView<K, TV, CV> originalTxView = getTxView(true);
    return new JacisStoreTxView<>(withTxName, originalTxView);
  }

  @Override
  public void startReadOnlyTransactionWithContext(JacisReadOnlyTransactionContext readOnlyTxContext) {
    if (!(readOnlyTxContext instanceof JacisStoreTxView)) {
      throw new IllegalArgumentException("Passed illegal transactional context: " + readOnlyTxContext);
    }
    JacisStoreTxView<K, TV, CV> oldTxView = getTxView(false);
    if (oldTxView != null) {
      throw new IllegalStateException("Failed to start a read only transaction while another transaction is active! Active transaction: " + oldTxView.getTransaction() + ", passed transaction context: " + readOnlyTxContext + ", Thread: " + Thread.currentThread().getName());
    }
    @SuppressWarnings("unchecked") JacisStoreTxView<K, TV, CV> newTx = (JacisStoreTxView<K, TV, CV>) readOnlyTxContext;
    setTransactionContext(newTx);
  }

  @Override
  public boolean containsKey(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.isNotNull();
    }
    StoreEntry<K, TV, CV> coreEntry = store.get(key);
    return coreEntry != null && coreEntry.isNotNull();
  }

  @Override
  public boolean isUpdated(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isUpdated();
  }

  @Override
  public boolean isStale(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isStale(txView);
  }

  @Override
  public void checkStale(K key) throws JacisStaleObjectException {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      entryTxView.assertNotStale(txView);
    }
  }

  @Override
  public TV get(K key) {
    return getOrCreateEntryTxView(getOrCreateTxView(), key).getValue();
  }

  @Override
  public TV getReadOnly(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.getValue();
    } else {
      StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
      return committedEntry == null ? null : objectAdapter.cloneCommitted2ReadOnlyTxView(committedEntry.getValue());
    }
  }

  @Override
  public <P> P getProjectionReadOnly(K key, Function<TV, P> projection) {
    return projection.apply(getReadOnly(key));
  }

  /** @return a stream of all keys currently stored in the store. Note that the keys added by any pending transactions are contained (with null values if not jet committed).  */
  private Stream<K> keyStream() {
    return store.keySet().stream(); // store contains also new entries (with null value)! Therefore iterating the keys is usually enough
  }

  @Override
  public Stream<TV> stream() { // Note this method will clone all objects into the TX view!
    return keyStream().map(this::get).filter(v -> v != null);
  }

  @Override
  public Stream<TV> streamReadOnly() {
    return keyStream().map(this::getReadOnly).filter(v -> v != null);
  }

  @Override
  public Stream<TV> stream(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(k -> pair(k, getReadOnly(k))).filter(e -> e.val != null && filter.test(e.val)).map(e -> get(e.key));
    } else {
      return stream();
    }
  }

  @Override
  public Stream<TV> streamReadOnly(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(this::getReadOnly).filter(v -> v != null && filter.test(v));
    } else {
      return streamReadOnly();
    }
  }

  @Override
  public List<TV> getAll() {
    return getAll(null);
  }

  @Override
  public List<TV> getAll(Predicate<TV> filter) {
    return stream(filter).collect(Collectors.toList());
  }

  @Override
  public List<TV> getAllReadOnly() {
    return getAllReadOnly(null);
  }

  @Override
  public List<TV> getAllReadOnly(Predicate<TV> filter) {
    return streamReadOnly(filter).collect(Collectors.toList());
  }

  @Override
  public List<TV> getAllAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAll(filter));
  }

  @Override
  public List<TV> getAllReadOnlyAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAllReadOnly(filter));
  }

  @Override
  public void update(K key, TV value) throws JacisTransactionAlreadyPreparedForCommitException {
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView().assertWritable();
    if (txView.isCommitPending()) {
      throw new JacisTransactionAlreadyPreparedForCommitException("Failed to update " + key + " because transaction is already prepared for commit: " + txView);
    }
    StoreEntryTxView<K, TV, CV> entryTxView = getOrCreateEntryTxView(txView, key);
    entryTxView.updateValue(value);
  }

  @Override
  public void remove(K key) {
    update(key, null);
  }

  @Override
  public TV refresh(K key) { // refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, true);
    return get(key);
  }

  @Override
  public TV refreshIfNotUpdated(K key) { // if not updated: refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, false);
    return get(key);
  }

  @Override
  public int size() { // heuristic (due to concurrent access)
    return store.size();
  }

  @Override
  public void executeAtomic(Runnable atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  @Override
  public <R> R computeAtomic(Supplier<R> atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    return withReadLock(atomicOperation);
  }

  @Override
  public <C> C accumulate(C target, BiConsumer<C, TV> accumulator) {
    for (TV entryTxView : getAllReadOnly(null)) {
      accumulator.accept(target, entryTxView);
    }
    return target;
  }

  @Override
  public <C> C accumulateAtomic(C target, BiConsumer<C, TV> accumulator) {
    return computeAtomic(() -> accumulate(target, accumulator));
  }

  @Override
  public TV getTransactionStartValue(K key) {
    assertTrackOriginalValue();
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView == null ? null : entryTxView.getOrigValue(); // if TX never touched the object we return null
  }

  @Override
  public StoreEntryInfo<K, TV> getObjectInfo(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return new StoreEntryInfo<>(key, committedEntry, entryTxView, txView);
  }

  @Override
  public synchronized void clear() {
    storeAccessLock.writeLock().lock();// <======= **WRITE** LOCK =====
    try {
      for (JacisStoreTxView<K, TV, CV> txCtx : txViewMap.values()) {
        txCtx.invalidate("store cleared");
      }
      store.clear();
      trackedViewRegistry.clearViews();
    } finally {
      storeAccessLock.writeLock().unlock();// <======= **WRITE** UNLOCK =====
    }
  }

  //======================================================================================
  // transaction demarcation methods
  //======================================================================================

  @Override
  protected void internalPrepare(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executePrepare(this, transaction)));
  }

  @Override
  protected void internalCommit(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeCommit(this, transaction)));
  }

  @Override
  protected void internalRollback(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeRollback(this, transaction)));
  }

  //======================================================================================
  // helper methods to deal with transaction views of entries
  //======================================================================================

  private StoreEntryTxView<K, TV, CV> getOrCreateEntryTxView(JacisStoreTxView<K, TV, CV> txView, K key) {
    StoreEntryTxView<K, TV, CV> entryTxView = txView.getEntryTxView(key);
    if (entryTxView == null) {
      entryTxView = withWriteLock(() -> txView.createTxViewEntry(getOrCreateCommittedEntry(key)));
    }
    return entryTxView;
  }

  //======================================================================================
  // helper methods to deal with committed entries
  //======================================================================================

  private StoreEntry<K, TV, CV> createCommittedEntry(K key) {
    StoreEntry<K, TV, CV> newCommittedEntry = new StoreEntry<>(this, key);
    StoreEntry<K, TV, CV> oldCommittedEntry = store.putIfAbsent(key, newCommittedEntry); // safe if another TX created one in the meantime
    return oldCommittedEntry != null ? oldCommittedEntry : newCommittedEntry;
  }

  private StoreEntry<K, TV, CV> getCommittedEntry(K key) {
    return store.get(key);
  }

  private StoreEntry<K, TV, CV> getOrCreateCommittedEntry(K key) {
    StoreEntry<K, TV, CV> committedEntry = store.get(key);
    if (committedEntry == null) {
      committedEntry = createCommittedEntry(key);
    }
    return committedEntry;
  }

  void checkRemoveCommittedEntry(StoreEntry<K, TV, CV> entryCommitted, JacisStoreTxView<K, TV, CV> currTxView) {
    if (entryCommitted.getValue() != null || entryCommitted.isLocked()) {
      return; // cannot remove
    }
    K key = entryCommitted.getKey();
    Collection<JacisStoreTxView<K, TV, CV>> txs;
    synchronized (txViewMap) {
      txs = new ArrayList<>(txViewMap.values());
    }
    for (JacisStoreTxView<K, TV, CV> txCtx : txs) {
      if (txCtx.isReadOnly()) {
        continue;
      } else if (currTxView.getTransaction().equals(txCtx.getTransaction())) {
        continue; // the current transaction referring a core entry can be ignored
      }
      if (txCtx.containsTxView(key)) {
        return; // still referred by transaction
      }
    }
    store.remove(key);
  }

  //======================================================================================
  // synchronized execution
  //======================================================================================

  private <R> R withWriteLock(Supplier<R> task) {
    storeAccessLock.writeLock().lock(); // <======= **WRITE** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.writeLock().unlock(); // <======= **WRITE** UNLOCK =====
    }
  }

  private <R> R withReadLock(Supplier<R> task) {
    storeAccessLock.readLock().lock(); // <======= **WRITE** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.readLock().unlock(); // <======= **WRITE** UNLOCK =====
    }
  }

  private Supplier<Object> runnableWrapper(Runnable r) {
    return () -> {
      r.run();
      return null;
    };
  }

  //======================================================================================
  // private methods to maintain the TX view
  //======================================================================================

  JacisStoreTxView<K, TV, CV> getTxView() {
    return getTxView(false);
  }

  private JacisStoreTxView<K, TV, CV> getOrCreateTxView() {
    return getTxView(true);
  }

  private JacisStoreTxView<K, TV, CV> getTxView(boolean createIfAbsent) {
    JacisTransactionHandle transaction = container.getCurrentTransaction(createIfAbsent);
    return getTxView(transaction, createIfAbsent);
  }

  JacisStoreTxView<K, TV, CV> getTxView(JacisTransactionHandle transaction, boolean createIfAbsent) {
    JacisStoreTxView<K, TV, CV> txView = txViewMap.get(transaction);
    if (txView == null && createIfAbsent) {
      txView = new JacisStoreTxView<>(this, transaction);
      txViewMap.put(transaction, txView);
    }
    return txView;
  }

  private void setTransactionContext(JacisStoreTxView<K, TV, CV> newTxContext) {
    JacisTransactionHandle transaction = container.getCurrentTransaction(true);
    txViewMap.put(transaction, newTxContext);
  }

  void notifyTxViewDestroyed(JacisStoreTxView<K, TV, CV> txView) {
    txViewMap.remove(txView.getTransaction());
  }

  //======================================================================================
  // other private helper methods
  //======================================================================================

  private void assertTrackOriginalValue() {
    if (!spec.isTrackOriginalValueEnabled()) {
      throw new UnsupportedOperationException("Track original values not supported by " + spec + "!");
    }
  }

  private KeyValuePair<K, TV> pair(K key, TV val) {
    return new KeyValuePair<>(key, val);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "-(" + spec + ": #" + store.size() + " entries)";
  }

  private static class KeyValuePair<PK, PV> {
    PK key;
    PV val;

    KeyValuePair(PK key, PV val) {
      this.key = key;
      this.val = val;
    }

  }

}
