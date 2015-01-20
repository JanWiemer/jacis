/**
 * Copyright (c) 2010 Jan Wiemer
 */
package org.jacis.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.plugin.JacisModificationListener;

/**
 * @author Jan Wiemer
 * 
 * Storing a single type of objects.
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public class JacisStore<K, V> {

  private final JacisContainer container;
  private final StoreIdentifier storeIdentifier;
  private final JacisObjectTypeSpec<K, V> spec;
  private final ConcurrentHashMap<K, StoreEntry<K, V>> store = new ConcurrentHashMap<>();
  private final Map<JacisTransactionHandle, JacisStoreTxView<K, V>> txViewMap = Collections.synchronizedMap(new WeakHashMap<JacisTransactionHandle, JacisStoreTxView<K, V>>());
  private final ReadWriteLock storeAccessLock = new ReentrantReadWriteLock(true); // lock to synchronize changes on the committed entries of the store (specially during commit)
  private final StoreEntryCloneHelper<V> cloneHelper;
  private final TrackedViewRegistry<K, V> trackedViewRegistry;
  private final List<JacisModificationListener<K, V>> modificationListeners = new ArrayList<>();

  public JacisStore(JacisContainer container, StoreIdentifier storeIdentifier, JacisObjectTypeSpec<K, V> spec) {
    this.container = container;
    this.storeIdentifier = storeIdentifier;
    this.spec = spec;
    this.cloneHelper = new StoreEntryCloneHelper<>(spec);
    this.trackedViewRegistry = new TrackedViewRegistry<>(this, spec.isCheckViewsOnCommit());
    registerModificationListener(trackedViewRegistry);
  }

  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  public JacisContainer getContainer() {
    return container;
  }

  public JacisObjectTypeSpec<K, V> getObjectTypeSpec() {
    return spec;
  }

  public JacisStore<K, V> registerModificationListener(JacisModificationListener<K, V> listener) {
    modificationListeners.add(listener);
    return this;
  }

  public List<JacisModificationListener<K, V>> getModificationListeners() {
    return modificationListeners;
  }

  public StoreEntryCloneHelper<V> getCloneHelper() {
    return cloneHelper;
  }

  public TrackedViewRegistry<K, V> getTrackedViewRegistry() {
    return trackedViewRegistry;
  }

  public void prepare(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executePrepare(this, transaction)));
  }

  public void commit(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeCommit(this, transaction)));
  }

  public void rollback(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeRollback(this, transaction)));
  }

  public boolean containsKey(K key) {
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.isNotNull();
    }
    StoreEntry<K, V> coreEntry = store.get(key);
    return coreEntry != null && coreEntry.isNotNull();
  }

  public boolean isUpdated(K key) {
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isUpdated();
  }

  public boolean isStale(K key) {
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isStale(txView);
  }

  public V get(K key) {
    return getOrCreateEntryTxView(getOrCreateTxView(), key).getValue();
  }

  public V getReadOnly(K key) {
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.getValue();
    } else {
      StoreEntry<K, V> committedEntry = getCommittedEntry(key);
      return committedEntry == null ? null : committedEntry.getValue();
    }
  }

  public <P> P getProjectionReadOnly(K key, Function<V, P> projection) {
    return projection.apply(getReadOnly(key));
  }

  private Stream<K> keyStream() {
    return store.keySet().stream(); // store contains also new entries (with null value)! Therefore iterating the keys is usually enough
  }

  public Stream<V> stream() { // Note this method will clone all objects into the TX view!
    return keyStream().map(k -> get(k)).filter(v -> v != null);
  }

  public Stream<V> streamReadOnly() {
    return keyStream().map(k -> getReadOnly(k)).filter(v -> v != null);
  }

  public Stream<V> stream(Predicate<V> filter) {
    if (filter != null) {
      return keyStream().map(k -> pair(k, getReadOnly(k))).filter(e -> e.val != null && filter.test(e.val)).map(e -> get(e.key));
    } else {
      return stream();
    }
  }

  public Stream<V> streamReadOnly(Predicate<V> filter) {
    if (filter != null) {
      return keyStream().map(k -> getReadOnly(k)).filter(v -> v != null && filter.test(v));
    } else {
      return streamReadOnly();
    }
  }

  public List<V> getAll(Predicate<V> filter) {
    return stream(filter).collect(Collectors.toList());
  }

  public List<V> getAllReadOnly(Predicate<V> filter) {
    return streamReadOnly(filter).collect(Collectors.toList());
  }

  public List<V> getAllAtomic(Predicate<V> filter) {
    return computeAtomic(() -> getAll(filter));
  }

  public List<V> getAllReadOnlyAtomic(Predicate<V> filter) {
    return computeAtomic(() -> getAllReadOnly(filter));
  }

  public void update(K key, V value) {
    JacisStoreTxView<K, V> txView = getOrCreateTxView().assertWritable();
    StoreEntryTxView<K, V> entryTxView = getOrCreateEntryTxView(txView, key);
    entryTxView.updateValue(value);
  }

  public void remove(K key) {
    update(key, null);
  }

  public V refresh(K key) { // refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, V> txView = getTxView();
    txView.removeTxViewEntry(key, true);
    return get(key);
  }

  public V refreshIfNotUpdated(K key) { // if not updated: refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, V> txView = getTxView();
    txView.removeTxViewEntry(key, false);
    return get(key);
  }

  public int size() { // heuristic (due to concurrent access)
    return store.size();
  }

  public void executeAtomic(Runnable atomicOperation) { // Execute an atomic operation. No commit of any other TX and no other atomic action will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  public <R> R computeAtomic(Supplier<R> atomicOperation) { // Execute an atomic operation. No commit of any other TX and no other atomic action will interleave.
    return withReadLock(atomicOperation);
  }

  public <C> C collect(C target, BiConsumer<C, V> accumulator) {
    for (V entryTxView : getAllReadOnly(null)) {
      accumulator.accept(target, entryTxView);
    }
    return target;
  }

  public <C> C collectAtomic(C target, BiConsumer<C, V> accumulator) {
    return computeAtomic(() -> collect(target, accumulator));
  }

  public V getTransactionStartValue(K key) { // value that was valid as the object was first accessed by the current TX (null if untouched).
    assertTrackOriginalValue();
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView == null ? null : entryTxView.getOrigValue(); // if TX never touched the object we return null
  }

  public StoreEntryInfo<K, V> getObjectInfo(K key) {
    JacisStoreTxView<K, V> txView = getTxView();
    StoreEntry<K, V> committedEntry = getCommittedEntry(key);
    StoreEntryTxView<K, V> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return new StoreEntryInfo<K, V>(key, committedEntry, entryTxView, txView);
  }

  //======================================================================================
  // helper methods to deal with transaction views of entries
  //======================================================================================

  private StoreEntryTxView<K, V> getOrCreateEntryTxView(JacisStoreTxView<K, V> txView, K key) {
    StoreEntryTxView<K, V> entryTxView = txView.getEntryTxView(key);
    if (entryTxView == null) {
      entryTxView = withWriteLock(() -> txView.createTxViewEntry(getOrCreateCommittedEntry(key)));
    }
    return entryTxView;
  }

  //======================================================================================
  // helper methods to deal with committed entries
  //======================================================================================

  private StoreEntry<K, V> createCommittedEntry(K key) {
    StoreEntry<K, V> newCommittedEntry = new StoreEntry<>(this, key);
    StoreEntry<K, V> oldCommittedEntry = store.putIfAbsent(key, newCommittedEntry); // safe if another TX created one in the meantime
    return oldCommittedEntry != null ? oldCommittedEntry : newCommittedEntry;
  }

  private StoreEntry<K, V> getCommittedEntry(K key) {
    return store.get(key);
  }

  private StoreEntry<K, V> getOrCreateCommittedEntry(K key) {
    StoreEntry<K, V> committedEntry = store.get(key);
    if (committedEntry == null) {
      committedEntry = createCommittedEntry(key);
    }
    return committedEntry;
  }

  void checkRemoveCommittedEntry(StoreEntry<K, V> entryCommitted, JacisStoreTxView<K, V> currTxView) {
    if (entryCommitted.getValue() != null || entryCommitted.isLocked()) {
      return; // cannot remove
    }
    K key = entryCommitted.getKey();
    Collection<JacisStoreTxView<K, V>> txs;
    synchronized (txViewMap) {
      txs = new ArrayList<>(txViewMap.values());
    }
    for (JacisStoreTxView<K, V> txCtx : txs) {
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

  JacisStoreTxView<K, V> getTxView() {
    return getTxView(false);
  }

  JacisStoreTxView<K, V> getOrCreateTxView() {
    return getTxView(true);
  }

  private JacisStoreTxView<K, V> getTxView(boolean createIfAbsent) {
    JacisTransactionHandle transaction = container.getCurrentTransaction(createIfAbsent);
    return getTxView(transaction, createIfAbsent);
  }

  JacisStoreTxView<K, V> getTxView(JacisTransactionHandle transaction, boolean createIfAbsent) {
    JacisStoreTxView<K, V> txView = txViewMap.get(transaction);
    if (txView == null && createIfAbsent) {
      txView = new JacisStoreTxView<K, V>(this, transaction);
      txViewMap.put(transaction, txView);
    }
    return txView;
  }

  void notifyTxViewDestroyed(JacisStoreTxView<K, V> txView) {
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

  private KeyValuePair<K, V> pair(K key, V val) {
    return new KeyValuePair<K, V>(key, val);
  }

  private static class KeyValuePair<K, V> {
    public K key;
    public V val;

    public KeyValuePair(K key, V val) {
      this.key = key;
      this.val = val;
    }

  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "-(" + spec + ": #" + store.size() + " entries)";
  }

}
