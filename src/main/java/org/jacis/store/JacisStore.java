/*
 * Copyright (c) 2016. Jan Wiemer
 */
package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
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
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public class JacisStore<K, TV, CV> {

  private final JacisContainer container;
  private final StoreIdentifier storeIdentifier;
  private final JacisObjectTypeSpec<K, TV, CV> spec;
  private final ConcurrentHashMap<K, StoreEntry<K, TV, CV>> store = new ConcurrentHashMap<>();
  private final Map<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>> txViewMap = Collections.synchronizedMap(new WeakHashMap<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>>());
  private final ReadWriteLock storeAccessLock = new ReentrantReadWriteLock(true); // lock to synchronize changes on the committed entries of the store (specially during internalCommit)
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  private final TrackedViewRegistry<K, TV, CV> trackedViewRegistry;
  private final List<JacisModificationListener<K, TV>> modificationListeners = new ArrayList<>();

  public JacisStore(JacisContainer container, StoreIdentifier storeIdentifier, JacisObjectTypeSpec<K, TV, CV> spec) {
    this.container = container;
    this.storeIdentifier = storeIdentifier;
    this.spec = spec;
    this.objectAdapter = spec.getObjectAdapter();
    this.trackedViewRegistry = new TrackedViewRegistry<>(this, spec.isCheckViewsOnCommit());
    registerModificationListener(trackedViewRegistry);
  }

  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  public JacisContainer getContainer() {
    return container;
  }

  public JacisObjectTypeSpec<K, TV, CV> getObjectTypeSpec() {
    return spec;
  }

  public JacisStore<K, TV, CV> registerModificationListener(JacisModificationListener<K, TV> listener) {
    if (!getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering modification listeners is only supported if original values are tracked, but they are not tracked for " + this + "! Trying to register listener: " + listener);
    }
    modificationListeners.add(listener);
    return this;
  }

  public List<JacisModificationListener<K, TV>> getModificationListeners() {
    return modificationListeners;
  }

  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  public TrackedViewRegistry<K, TV, CV> getTrackedViewRegistry() {
    return trackedViewRegistry;
  }

  public void internalPrepare(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executePrepare(this, transaction)));
  }

  public void internalCommit(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeCommit(this, transaction)));
  }

  public void internalRollback(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeRollback(this, transaction)));
  }

  public boolean containsKey(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.isNotNull();
    }
    StoreEntry<K, TV, CV> coreEntry = store.get(key);
    return coreEntry != null && coreEntry.isNotNull();
  }

  public boolean isUpdated(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isUpdated();
  }

  public boolean isStale(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isStale(txView);
  }

  public void checkStale(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      entryTxView.assertNotStale(txView);
    }
  }

  public TV get(K key) {
    return getOrCreateEntryTxView(getOrCreateTxView(), key).getValue();
  }

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

  public <P> P getProjectionReadOnly(K key, Function<TV, P> projection) {
    return projection.apply(getReadOnly(key));
  }

  private Stream<K> keyStream() {
    return store.keySet().stream(); // store contains also new entries (with null value)! Therefore iterating the keys is usually enough
  }

  public Stream<TV> stream() { // Note this method will clone all objects into the TX view!
    return keyStream().map(k -> get(k)).filter(v -> v != null);
  }

  public Stream<TV> streamReadOnly() {
    return keyStream().map(k -> getReadOnly(k)).filter(v -> v != null);
  }

  public Stream<TV> stream(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(k -> pair(k, getReadOnly(k))).filter(e -> e.val != null && filter.test(e.val)).map(e -> get(e.key));
    } else {
      return stream();
    }
  }

  public Stream<TV> streamReadOnly(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(k -> getReadOnly(k)).filter(v -> v != null && filter.test(v));
    } else {
      return streamReadOnly();
    }
  }

  public List<TV> getAll(Predicate<TV> filter) {
    return stream(filter).collect(Collectors.toList());
  }

  public List<TV> getAllReadOnly(Predicate<TV> filter) {
    return streamReadOnly(filter).collect(Collectors.toList());
  }

  public List<TV> getAllAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAll(filter));
  }

  public List<TV> getAllReadOnlyAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAllReadOnly(filter));
  }

  public void update(K key, TV value) {
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView().assertWritable();
    StoreEntryTxView<K, TV, CV> entryTxView = getOrCreateEntryTxView(txView, key);
    entryTxView.updateValue(value);
  }

  public void remove(K key) {
    update(key, null);
  }

  public TV refresh(K key) { // refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, true);
    return get(key);
  }

  public TV refreshIfNotUpdated(K key) { // if not updated: refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, false);
    return get(key);
  }

  public int size() { // heuristic (due to concurrent access)
    return store.size();
  }

  public void executeAtomic(Runnable atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  public <R> R computeAtomic(Supplier<R> atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    return withReadLock(atomicOperation);
  }

  public <C> C collect(C target, BiConsumer<C, TV> accumulator) {
    for (TV entryTxView : getAllReadOnly(null)) {
      accumulator.accept(target, entryTxView);
    }
    return target;
  }

  public <C> C collectAtomic(C target, BiConsumer<C, TV> accumulator) {
    return computeAtomic(() -> collect(target, accumulator));
  }

  public TV getTransactionStartValue(K key) { // value that was valid as the object was first accessed by the current TX (null if untouched).
    assertTrackOriginalValue();
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView == null ? null : entryTxView.getOrigValue(); // if TX never touched the object we return null
  }

  public StoreEntryInfo<K, TV, CV> getObjectInfo(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return new StoreEntryInfo<>(key, committedEntry, entryTxView, txView);
  }

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

  JacisStoreTxView<K, TV, CV> getOrCreateTxView() {
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
    public PK key;
    public PV val;

    public KeyValuePair(PK key, PV val) {
      this.key = key;
      this.val = val;
    }

  }

}
